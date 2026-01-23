"""
Stock Data Loader for Supabase
Fetches daily stock data and loads it into Supabase database
"""

import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockDataLoader:
    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize the stock data loader with Supabase credentials"""
        try:
            # Try new version first
            self.supabase: Client = create_client(supabase_url, supabase_key)
        except TypeError:
            # Fall back for older versions
            from supabase.client import ClientOptions
            self.supabase: Client = create_client(
                supabase_url, 
                supabase_key,
                options=ClientOptions()
            )
        logger.info("Connected to Supabase")
    
    def load_tickers_from_csv(self, csv_path: str) -> List[str]:
        """Load ticker symbols from CSV file"""
        try:
            df = pd.read_csv(csv_path)
            # Assume the CSV has a column named 'ticker' or 'symbol'
            ticker_column = None
            for col in ['ticker', 'Ticker', 'symbol', 'Symbol', 'TICKER', 'SYMBOL']:
                if col in df.columns:
                    ticker_column = col
                    break
            
            if ticker_column is None:
                # If no standard column found, use the first column
                ticker_column = df.columns[0]
                logger.warning(f"No standard ticker column found, using first column: {ticker_column}")
            
            tickers = df[ticker_column].dropna().astype(str).str.strip().tolist()
            logger.info(f"Loaded {len(tickers)} tickers from {csv_path}")
            return tickers
        except Exception as e:
            logger.error(f"Error loading tickers from CSV: {e}")
            return []
    
    def insert_tickers_to_db(self, tickers: List[str]):
        """Insert tickers into the tickers table"""
        try:
            # Fetch company info and insert tickers
            ticker_data = []
            for ticker in tickers:
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    ticker_data.append({
                        'ticker': ticker.upper(),
                        'company_name': info.get('longName', info.get('shortName', ticker)),
                        'sector': info.get('sector', 'Unknown'),
                        'active': True
                    })
                    logger.info(f"Prepared ticker data for {ticker}")
                except Exception as e:
                    logger.warning(f"Could not get info for {ticker}, adding with basic data: {e}")
                    ticker_data.append({
                        'ticker': ticker.upper(),
                        'company_name': ticker,
                        'sector': 'Unknown',
                        'active': True
                    })
                
                # Add delay to avoid rate limiting
                time.sleep(0.1)
            
            # Insert in batches
            batch_size = 50
            for i in range(0, len(ticker_data), batch_size):
                batch = ticker_data[i:i+batch_size]
                try:
                    # Use upsert to handle duplicates
                    self.supabase.table('tickers').upsert(batch, on_conflict='ticker').execute()
                    logger.info(f"Inserted batch {i//batch_size + 1} ({len(batch)} tickers)")
                except Exception as e:
                    logger.error(f"Error inserting ticker batch: {e}")
            
            logger.info(f"Successfully inserted/updated {len(ticker_data)} tickers")
        except Exception as e:
            logger.error(f"Error in insert_tickers_to_db: {e}")
    
    def fetch_stock_data(self, ticker: str, date: Optional[datetime] = None, max_retries: int = 3) -> Optional[Dict]:
        """Fetch stock data for a specific ticker and date"""
        for attempt in range(max_retries):
            try:
                stock = yf.Ticker(ticker)
                
                # If no date provided, use yesterday (most recent complete trading day)
                if date is None:
                    date = datetime.now() - timedelta(days=1)
                
                # Fetch data for the specific date (need a small range)
                start_date = date - timedelta(days=5)  # Get a few days to ensure we get the data
                end_date = date + timedelta(days=1)
                
                hist = stock.history(start=start_date, end=end_date)
                
                if hist.empty:
                    logger.warning(f"No data available for {ticker} on {date.date()}")
                    return None
                
                # Get the most recent data point
                latest_data = hist.iloc[-1]
                latest_date = hist.index[-1].date()
                
                # Get company info for market cap
                info = stock.info
                market_cap = info.get('marketCap', None)
                company_name = info.get('longName', info.get('shortName', ticker))
                
                stock_data = {
                    'ticker': ticker.upper(),
                    'company_name': company_name,
                    'date': str(latest_date),
                    'open_price': float(latest_data['Open']),
                    'high_price': float(latest_data['High']),
                    'low_price': float(latest_data['Low']),
                    'close_price': float(latest_data['Close']),
                    'volume': int(latest_data['Volume']),
                    'market_cap': market_cap,
                    'created_at': datetime.now().isoformat()
                }
                
                return stock_data
            
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error fetching {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                else:
                    logger.error(f"Error fetching data for {ticker} after {max_retries} attempts: {e}")
                    return None
    
    def insert_stock_data(self, stock_data: Dict):
        """Insert stock data into Supabase"""
        try:
            # Use upsert to handle duplicates (same ticker and date)
            self.supabase.table('stocks_daily').upsert(
                stock_data, 
                on_conflict='ticker,date'
            ).execute()
            logger.info(f"Inserted data for {stock_data['ticker']} on {stock_data['date']}")
        except Exception as e:
            logger.error(f"Error inserting stock data for {stock_data['ticker']}: {e}")
    
    def calculate_and_insert_daily_movers(self, min_percent: float = 15.0):
        """Calculate daily movers (stocks that moved 15%+ in one day)"""
        try:
            # Get today's date
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            # Fetch stocks from the last 2 days
            logger.info("Calculating daily movers...")
            
            # Query stocks for yesterday and the day before
            two_days_ago = yesterday - timedelta(days=1)
            
            response = self.supabase.table('stocks_daily')\
                .select('*')\
                .gte('date', str(two_days_ago))\
                .lte('date', str(yesterday))\
                .execute()
            
            if not response.data:
                logger.warning("No stock data found for daily mover calculation")
                return
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(response.data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Sort by ticker and date
            df = df.sort_values(['ticker', 'date'])
            
            # Calculate daily percentage change
            movers = []
            for ticker in df['ticker'].unique():
                ticker_data = df[df['ticker'] == ticker].sort_values('date')
                
                if len(ticker_data) < 2:
                    continue
                
                # Get last two days
                prev_close = ticker_data.iloc[-2]['close_price']
                curr_close = ticker_data.iloc[-1]['close_price']
                curr_date = ticker_data.iloc[-1]['date'].date()
                
                percent_change = ((curr_close - prev_close) / prev_close) * 100
                
                if abs(percent_change) >= min_percent:
                    movers.append({
                        'ticker': ticker,
                        'date': str(curr_date),
                        'previous_close': float(prev_close),
                        'current_close': float(curr_close),
                        'percent_change': float(round(percent_change, 2)),
                        'volume': int(ticker_data.iloc[-1]['volume']),
                        'created_at': datetime.now().isoformat()
                    })
            
            # Insert movers
            if movers:
                for mover in movers:
                    try:
                        self.supabase.table('daily_movers').upsert(
                            mover,
                            on_conflict='ticker,date'
                        ).execute()
                    except Exception as e:
                        logger.error(f"Error inserting daily mover {mover['ticker']}: {e}")
                
                logger.info(f"Found and inserted {len(movers)} daily movers (±{min_percent}%)")
            else:
                logger.info(f"No stocks moved ±{min_percent}% today")
        
        except Exception as e:
            logger.error(f"Error calculating daily movers: {e}")
    
    def calculate_and_insert_weekly_movers(self, min_percent: float = 15.0):
        """Calculate weekly movers (stocks that moved 15%+ in one week)"""
        try:
            today = datetime.now().date()
            week_ago = today - timedelta(days=7)
            
            logger.info("Calculating weekly movers...")
            
            # Fetch stocks from the last 10 days (to ensure we get full week)
            ten_days_ago = today - timedelta(days=10)
            
            response = self.supabase.table('stocks_daily')\
                .select('*')\
                .gte('date', str(ten_days_ago))\
                .lte('date', str(today))\
                .execute()
            
            if not response.data:
                logger.warning("No stock data found for weekly mover calculation")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(response.data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Sort by ticker and date
            df = df.sort_values(['ticker', 'date'])
            
            # Calculate weekly percentage change
            movers = []
            for ticker in df['ticker'].unique():
                ticker_data = df[df['ticker'] == ticker].sort_values('date')
                
                if len(ticker_data) < 5:  # Need at least 5 trading days
                    continue
                
                # Get oldest and newest in the week
                week_start_close = ticker_data.iloc[0]['close_price']
                week_end_close = ticker_data.iloc[-1]['close_price']
                week_start_date = ticker_data.iloc[0]['date'].date()
                week_end_date = ticker_data.iloc[-1]['date'].date()
                
                # Only calculate if we have roughly a week's worth of data
                days_diff = (week_end_date - week_start_date).days
                if days_diff < 5 or days_diff > 9:
                    continue
                
                percent_change = ((week_end_close - week_start_close) / week_start_close) * 100
                
                if abs(percent_change) >= min_percent:
                    movers.append({
                        'ticker': ticker,
                        'week_start_date': str(week_start_date),
                        'week_end_date': str(week_end_date),
                        'week_start_close': float(week_start_close),
                        'week_end_close': float(week_end_close),
                        'percent_change': float(round(percent_change, 2)),
                        'created_at': datetime.now().isoformat()
                    })
            
            # Insert movers
            if movers:
                for mover in movers:
                    try:
                        self.supabase.table('weekly_movers').upsert(
                            mover,
                            on_conflict='ticker,week_end_date'
                        ).execute()
                    except Exception as e:
                        logger.error(f"Error inserting weekly mover {mover['ticker']}: {e}")
                
                logger.info(f"Found and inserted {len(movers)} weekly movers (±{min_percent}%)")
            else:
                logger.info(f"No stocks moved ±{min_percent}% this week")
        
        except Exception as e:
            logger.error(f"Error calculating weekly movers: {e}")
    
    def run_daily_update(self, tickers: List[str], batch_delay: float = 1.5, batch_size: int = 100):
        """Run the daily update for all tickers"""
        logger.info(f"Starting daily update for {len(tickers)} tickers")
        
        successful = 0
        failed = 0
        
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"Processing {i}/{len(tickers)}: {ticker}")
            
            stock_data = self.fetch_stock_data(ticker)
            
            if stock_data:
                self.insert_stock_data(stock_data)
                successful += 1
            else:
                failed += 1
            
            # Add delay to avoid rate limiting
            time.sleep(batch_delay)
            
            # Progress checkpoint every batch_size tickers
            if i % batch_size == 0:
                logger.info(f"Progress checkpoint: {i}/{len(tickers)} processed. Success: {successful}, Failed: {failed}")
                # Optional: Add a longer pause every batch
                time.sleep(5)
        
        logger.info(f"Daily update complete. Success: {successful}, Failed: {failed}")
        
        # After updating all stocks, calculate movers
        if successful > 0:
            logger.info("Calculating daily and weekly movers...")
            self.calculate_and_insert_daily_movers()
            self.calculate_and_insert_weekly_movers()
        else:
            logger.warning("No successful updates, skipping mover calculations")


def main():
    """Main function to run the stock data loader"""
    
    # Get Supabase credentials from environment variables
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    TICKERS_CSV = os.getenv('TICKERS_CSV', 'tickers.csv')
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Please set SUPABASE_URL and SUPABASE_KEY environment variables")
        return
    
    # Initialize loader
    loader = StockDataLoader(SUPABASE_URL, SUPABASE_KEY)
    
    # Load tickers from CSV
    tickers = loader.load_tickers_from_csv(TICKERS_CSV)
    
    if not tickers:
        logger.error("No tickers loaded. Please check your CSV file.")
        return
    
    # Optional: Insert tickers into database (run once or when adding new tickers)
    # Uncomment the line below if you want to populate the tickers table
    # loader.insert_tickers_to_db(tickers)
    
    # Run daily update
    loader.run_daily_update(tickers)


if __name__ == "__main__":
    main()
