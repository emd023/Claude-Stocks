"""
Fast Stock Data Loader for Supabase
Based on proven batch download approach - processes 10,000 tickers in ~10 minutes
Optimized for daily updates with minimal API calls
"""

import os
from datetime import date, timedelta
from typing import List, Dict, Any
import sys

import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import yfinance as yf
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 150  # Download this many tickers at once
UPSERT_CHUNK = 800  # Upsert this many records at once

def get_supabase_client() -> Client:
    """Initialize Supabase client"""
    load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        logger.error("SUPABASE_URL and SUPABASE_KEY must be set in environment")
        sys.exit(1)
    
    try:
        return create_client(url, key)
    except TypeError:
        from supabase.client import ClientOptions
        return create_client(url, key, options=ClientOptions())

def get_tickers_from_db(client: Client) -> pd.DataFrame:
    """
    Fetch all active tickers from database with pagination
    Returns: DataFrame with columns [ticker, company_name]
    """
    page_size = 1000
    offset = 0
    all_rows = []
    
    logger.info("Loading tickers from database...")
    
    while True:
        try:
            response = client.table("tickers") \
                .select("ticker,company_name,active") \
                .eq("active", True) \
                .order("ticker") \
                .range(offset, offset + page_size - 1) \
                .execute()
            
            page_data = response.data
            
            if not page_data:
                break
            
            all_rows.extend(page_data)
            
            if len(page_data) < page_size:
                break
                
            offset += page_size
            
        except Exception as e:
            logger.warning(f"Error fetching tickers at offset {offset}: {e}")
            break
    
    if not all_rows:
        logger.error("No active tickers found in database")
        sys.exit(1)
    
    df = pd.DataFrame(all_rows)
    logger.info(f"Loaded {len(df)} tickers from database")
    
    return df[["ticker", "company_name"]]

def load_tickers_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Load tickers from CSV file
    Returns: DataFrame with columns [ticker, company_name]
    """
    try:
        df = pd.read_csv(csv_path)
        
        # Find ticker column
        ticker_col = None
        for col in ['ticker', 'Ticker', 'symbol', 'Symbol', 'TICKER', 'SYMBOL']:
            if col in df.columns:
                ticker_col = col
                break
        
        if ticker_col is None:
            ticker_col = df.columns[0]
            logger.warning(f"Using first column as ticker: {ticker_col}")
        
        tickers = df[ticker_col].dropna().astype(str).str.strip().tolist()
        
        # Create company name column if it doesn't exist
        if 'company_name' in df.columns or 'name' in df.columns:
            name_col = 'company_name' if 'company_name' in df.columns else 'name'
            names = df[name_col].fillna(df[ticker_col]).tolist()
        else:
            names = tickers
        
        result_df = pd.DataFrame({
            'ticker': tickers,
            'company_name': names
        })
        
        logger.info(f"Loaded {len(result_df)} tickers from {csv_path}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        sys.exit(1)

def get_last_market_day() -> date:
    """
    Determine the last trading day by checking SPY
    Falls back to yesterday if SPY data unavailable
    """
    try:
        hist = yf.download("SPY", period="7d", interval="1d", progress=False, auto_adjust=False)
        if not hist.empty:
            last_day = pd.to_datetime(hist.index[-1]).date()
            logger.info(f"Last market day: {last_day}")
            return last_day
    except Exception as e:
        logger.warning(f"Error getting last market day from SPY: {e}")
    
    # Fallback to yesterday
    yesterday = date.today() - timedelta(days=1)
    logger.info(f"Using fallback date: {yesterday}")
    return yesterday

def fetch_batch_data(tickers: List[str], target_date: date) -> pd.DataFrame:
    """
    Fetch one day of data for a batch of tickers
    Uses yfinance batch download for speed
    """
    if not tickers:
        return pd.DataFrame()
    
    try:
        # Download data for all tickers in batch
        data = yf.download(
            tickers=tickers,
            start=target_date.isoformat(),
            end=(target_date + timedelta(days=1)).isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        
        rows = []
        
        # Handle multi-ticker response
        if isinstance(data.columns, pd.MultiIndex):
            # Multiple tickers: columns are (ticker, field)
            unique_tickers = sorted(set(ticker for ticker, _ in data.columns))
            
            for ticker in unique_tickers:
                try:
                    ticker_data = data[ticker].reset_index()
                    ticker_data.columns = [col.lower().replace(' ', '_') for col in ticker_data.columns]
                    
                    if ticker_data.empty or ticker_data['close'].isna().all():
                        continue
                    
                    ticker_data['ticker'] = ticker
                    rows.append(ticker_data[['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']])
                except Exception as e:
                    logger.debug(f"Error processing {ticker}: {e}")
                    continue
        else:
            # Single ticker response
            ticker_data = data.reset_index()
            ticker_data.columns = [col.lower().replace(' ', '_') for col in ticker_data.columns]
            
            if not ticker_data.empty and not ticker_data['close'].isna().all():
                ticker_data['ticker'] = tickers[0]
                rows.append(ticker_data[['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']])
        
        if rows:
            result = pd.concat(rows, ignore_index=True)
            return result
        else:
            return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'ticker'])
            
    except Exception as e:
        logger.error(f"Error fetching batch: {e}")
        return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'ticker'])

def prepare_records(df: pd.DataFrame, ticker_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Convert DataFrame to JSON-serializable records for Supabase
    """
    if df.empty:
        return []
    
    # Add company names
    df['company_name'] = df['ticker'].map(ticker_map).fillna(df['ticker'])
    
    # Normalize data types
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    for col in ['open', 'high', 'low', 'close']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
    
    # Remove rows with missing critical data
    df = df.dropna(subset=['ticker', 'close'])
    
    # Build records
    records = []
    for _, row in df.iterrows():
        try:
            record = {
                'ticker': str(row['ticker']),
                'company_name': str(row['company_name']),
                'date': row['date'].isoformat(),
                'open_price': float(row['open']) if pd.notna(row.get('open')) else None,
                'high_price': float(row['high']) if pd.notna(row.get('high')) else None,
                'low_price': float(row['low']) if pd.notna(row.get('low')) else None,
                'close_price': float(row['close']),
                'volume': int(row['volume']) if pd.notna(row.get('volume')) else 0,
                'market_cap': None,  # Not fetched for speed
                'created_at': pd.Timestamp.now().isoformat()
            }
            records.append(record)
        except Exception as e:
            logger.debug(f"Error preparing record for {row.get('ticker')}: {e}")
            continue
    
    return records

def upsert_records(client: Client, records: List[Dict[str, Any]], chunk_size: int = 800):
    """
    Upsert records to Supabase in chunks
    """
    if not records:
        return
    
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        try:
            client.table('stocks_daily').upsert(
                chunk,
                on_conflict='ticker,date'
            ).execute()
            logger.debug(f"Upserted chunk {i//chunk_size + 1} ({len(chunk)} records)")
        except Exception as e:
            logger.error(f"Error upserting chunk {i//chunk_size + 1}: {e}")
            raise

def calculate_daily_movers(client: Client, target_date: date, min_percent: float = 15.0):
    """Calculate and store daily movers"""
    try:
        logger.info("Calculating daily movers...")
        
        yesterday = target_date - timedelta(days=1)
        two_days_ago = yesterday - timedelta(days=3)  # Get a bit more to ensure we have previous day
        
        response = client.table('stocks_daily') \
            .select('*') \
            .gte('date', str(two_days_ago)) \
            .lte('date', str(target_date)) \
            .execute()
        
        if not response.data:
            logger.warning("No data for daily mover calculation")
            return
        
        df = pd.DataFrame(response.data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['ticker', 'date'])
        
        movers = []
        for ticker in df['ticker'].unique():
            ticker_data = df[df['ticker'] == ticker].sort_values('date')
            
            if len(ticker_data) < 2:
                continue
            
            # Get last two days
            prev_close = ticker_data.iloc[-2]['close_price']
            curr_close = ticker_data.iloc[-1]['close_price']
            curr_date = ticker_data.iloc[-1]['date'].date()
            
            if prev_close == 0 or pd.isna(prev_close) or pd.isna(curr_close):
                continue
            
            percent_change = ((curr_close - prev_close) / prev_close) * 100
            
            if abs(percent_change) >= min_percent:
                movers.append({
                    'ticker': ticker,
                    'date': str(curr_date),
                    'previous_close': float(prev_close),
                    'current_close': float(curr_close),
                    'percent_change': round(float(percent_change), 2),
                    'volume': int(ticker_data.iloc[-1]['volume']),
                    'created_at': pd.Timestamp.now().isoformat()
                })
        
        if movers:
            for mover in movers:
                try:
                    client.table('daily_movers').upsert(
                        mover,
                        on_conflict='ticker,date'
                    ).execute()
                except Exception as e:
                    logger.error(f"Error inserting mover {mover['ticker']}: {e}")
            
            logger.info(f"Found {len(movers)} stocks that moved ±{min_percent}%")
        else:
            logger.info(f"No stocks moved ±{min_percent}%")
            
    except Exception as e:
        logger.error(f"Error calculating daily movers: {e}")

def calculate_weekly_movers(client: Client, target_date: date, min_percent: float = 15.0):
    """Calculate and store weekly movers"""
    try:
        logger.info("Calculating weekly movers...")
        
        ten_days_ago = target_date - timedelta(days=10)
        
        response = client.table('stocks_daily') \
            .select('*') \
            .gte('date', str(ten_days_ago)) \
            .lte('date', str(target_date)) \
            .execute()
        
        if not response.data:
            logger.warning("No data for weekly mover calculation")
            return
        
        df = pd.DataFrame(response.data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['ticker', 'date'])
        
        movers = []
        for ticker in df['ticker'].unique():
            ticker_data = df[df['ticker'] == ticker].sort_values('date')
            
            if len(ticker_data) < 5:
                continue
            
            week_start_close = ticker_data.iloc[0]['close_price']
            week_end_close = ticker_data.iloc[-1]['close_price']
            week_start_date = ticker_data.iloc[0]['date'].date()
            week_end_date = ticker_data.iloc[-1]['date'].date()
            
            days_diff = (week_end_date - week_start_date).days
            if days_diff < 5 or days_diff > 9:
                continue
            
            if week_start_close == 0 or pd.isna(week_start_close) or pd.isna(week_end_close):
                continue
            
            percent_change = ((week_end_close - week_start_close) / week_start_close) * 100
            
            if abs(percent_change) >= min_percent:
                movers.append({
                    'ticker': ticker,
                    'week_start_date': str(week_start_date),
                    'week_end_date': str(week_end_date),
                    'week_start_close': float(week_start_close),
                    'week_end_close': float(week_end_close),
                    'percent_change': round(float(percent_change), 2),
                    'created_at': pd.Timestamp.now().isoformat()
                })
        
        if movers:
            for mover in movers:
                try:
                    client.table('weekly_movers').upsert(
                        mover,
                        on_conflict='ticker,week_end_date'
                    ).execute()
                except Exception as e:
                    logger.error(f"Error inserting weekly mover {mover['ticker']}: {e}")
            
            logger.info(f"Found {len(movers)} stocks that moved ±{min_percent}% over the week")
        else:
            logger.info(f"No stocks moved ±{min_percent}% over the week")
            
    except Exception as e:
        logger.error(f"Error calculating weekly movers: {e}")

def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Starting Fast Stock Data Loader")
    logger.info("="*60)
    
    # Initialize
    client = get_supabase_client()
    
    # Get target date
    target_date = get_last_market_day()
    logger.info(f"Target trading day: {target_date}")
    
    # Load tickers - try database first, fall back to CSV
    try:
        tickers_df = get_tickers_from_db(client)
        source = "database"
    except:
        csv_path = os.environ.get('TICKERS_CSV', 'tickers.csv')
        logger.info(f"Falling back to CSV: {csv_path}")
        tickers_df = load_tickers_from_csv(csv_path)
        source = "CSV"
    
    logger.info(f"Loaded {len(tickers_df)} tickers from {source}")
    
    # Create ticker to name mapping
    ticker_map = dict(zip(tickers_df['ticker'], tickers_df['company_name']))
    ticker_list = tickers_df['ticker'].tolist()
    
    # Process in batches
    total_records = 0
    total_batches = (len(ticker_list) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(ticker_list)} tickers in {total_batches} batches of {BATCH_SIZE}")
    
    for i in range(0, len(ticker_list), BATCH_SIZE):
        batch = ticker_list[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        logger.info(f"Batch {batch_num}/{total_batches}: Fetching {len(batch)} tickers...")
        
        # Fetch data
        df = fetch_batch_data(batch, target_date)
        
        if df.empty:
            logger.warning(f"Batch {batch_num}: No data returned")
            continue
        
        # Prepare records
        records = prepare_records(df, ticker_map)
        
        if records:
            # Upsert to database
            upsert_records(client, records, UPSERT_CHUNK)
            total_records += len(records)
            logger.info(f"Batch {batch_num}/{total_batches}: Upserted {len(records)} records")
        else:
            logger.warning(f"Batch {batch_num}: No valid records to upsert")
    
    logger.info(f"Data loading complete: {total_records} total records upserted")
    
    # Calculate percent changes for loaded data
    if total_records > 0:
        logger.info("Calculating percent changes (1D, 3D, 7D, 1M)...")
        try:
            # Call the SQL function to calculate percent changes
            client.rpc('calculate_percent_changes').execute()
            logger.info("✅ Percent changes calculated")
        except Exception as e:
            logger.warning(f"Could not calculate percent changes: {e}")
            logger.info("Make sure you've run the SQL migration (add_percent_change_columns.sql)")
    
    # Calculate movers if we got data
    if total_records > 0:
        calculate_daily_movers(client, target_date)
        calculate_weekly_movers(client, target_date)
    else:
        logger.warning("No data loaded - skipping mover calculations")
        sys.exit(1)  # Exit with error if no data
    
    logger.info("="*60)
    logger.info("Stock Data Loader Complete!")
    logger.info("="*60)

if __name__ == "__main__":
    main()
