"""
Stock Data Query Tool
Query and analyze stock data from Supabase
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd

load_dotenv()

class StockQueryTool:
    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize the query tool"""
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
    
    def get_daily_movers(self, min_percent: float = 15.0, start_date: str = None, end_date: str = None):
        """Get stocks that moved a certain percentage in a single day"""
        query = self.supabase.table('daily_movers').select('*')
        
        if min_percent:
            # Get stocks that moved at least min_percent (positive or negative)
            query = query.or_(f'percent_change.gte.{min_percent},percent_change.lte.{-min_percent}')
        
        if start_date:
            query = query.gte('date', start_date)
        
        if end_date:
            query = query.lte('date', end_date)
        
        response = query.order('date', desc=True).order('percent_change', desc=True).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()
    
    def get_weekly_movers(self, min_percent: float = 15.0, start_date: str = None):
        """Get stocks that moved a certain percentage in a week"""
        query = self.supabase.table('weekly_movers').select('*')
        
        if min_percent:
            query = query.or_(f'percent_change.gte.{min_percent},percent_change.lte.{-min_percent}')
        
        if start_date:
            query = query.gte('week_end_date', start_date)
        
        response = query.order('week_end_date', desc=True).order('percent_change', desc=True).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()
    
    def get_stock_history(self, ticker: str, start_date: str = None, end_date: str = None):
        """Get price history for a specific stock"""
        query = self.supabase.table('stocks_daily').select('*').eq('ticker', ticker.upper())
        
        if start_date:
            query = query.gte('date', start_date)
        
        if end_date:
            query = query.lte('date', end_date)
        
        response = query.order('date', desc=False).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()
    
    def get_stocks_by_custom_movement(self, start_date: str, end_date: str, min_percent: float = 15.0):
        """
        Get stocks that moved a certain percentage between any two dates
        Uses the custom PostgreSQL function created in the schema
        """
        try:
            # Call the PostgreSQL function
            response = self.supabase.rpc(
                'get_stocks_by_movement',
                {
                    'start_date': start_date,
                    'end_date': end_date,
                    'min_percent': min_percent
                }
            ).execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"Error calling function: {e}")
            return pd.DataFrame()
    
    def get_top_gainers(self, date: str = None, limit: int = 20):
        """Get top gaining stocks for a specific date"""
        if date is None:
            # Use yesterday's date
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        response = self.supabase.table('daily_movers')\
            .select('*')\
            .eq('date', date)\
            .gte('percent_change', 0)\
            .order('percent_change', desc=True)\
            .limit(limit)\
            .execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()
    
    def get_top_losers(self, date: str = None, limit: int = 20):
        """Get top losing stocks for a specific date"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        response = self.supabase.table('daily_movers')\
            .select('*')\
            .eq('date', date)\
            .lte('percent_change', 0)\
            .order('percent_change', desc=False)\
            .limit(limit)\
            .execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()
    
    def get_most_volatile_stocks(self, days: int = 30, limit: int = 20):
        """Get stocks with highest volatility (most frequent big moves)"""
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        response = self.supabase.table('daily_movers')\
            .select('ticker')\
            .gte('date', start_date)\
            .execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            # Count occurrences of each ticker
            volatility_counts = df['ticker'].value_counts().head(limit)
            return volatility_counts
        return pd.Series()
    
    def search_stocks(self, ticker_search: str = None, company_search: str = None):
        """Search for stocks by ticker or company name"""
        query = self.supabase.table('tickers').select('*')
        
        if ticker_search:
            query = query.ilike('ticker', f'%{ticker_search}%')
        
        if company_search:
            query = query.ilike('company_name', f'%{company_search}%')
        
        response = query.execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            return df
        return pd.DataFrame()


def main():
    """Example usage of the query tool"""
    
    # Initialize
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Please set SUPABASE_URL and SUPABASE_KEY environment variables")
        return
    
    query_tool = StockQueryTool(SUPABASE_URL, SUPABASE_KEY)
    
    print("=== Stock Data Query Examples ===\n")
    
    # Example 1: Get yesterday's top gainers
    print("1. Top 10 Gainers (Yesterday):")
    gainers = query_tool.get_top_gainers(limit=10)
    if not gainers.empty:
        print(gainers[['ticker', 'percent_change', 'current_close', 'volume']].to_string(index=False))
    else:
        print("No data available")
    print("\n" + "="*60 + "\n")
    
    # Example 2: Get yesterday's top losers
    print("2. Top 10 Losers (Yesterday):")
    losers = query_tool.get_top_losers(limit=10)
    if not losers.empty:
        print(losers[['ticker', 'percent_change', 'current_close', 'volume']].to_string(index=False))
    else:
        print("No data available")
    print("\n" + "="*60 + "\n")
    
    # Example 3: Get all stocks that moved 15%+ in the last 7 days
    print("3. Stocks that moved 15%+ (Last 7 Days):")
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    daily_movers = query_tool.get_daily_movers(min_percent=15.0, start_date=week_ago)
    if not daily_movers.empty:
        print(f"Found {len(daily_movers)} occurrences")
        print(daily_movers[['ticker', 'date', 'percent_change']].head(20).to_string(index=False))
    else:
        print("No stocks moved 15%+ in the last 7 days")
    print("\n" + "="*60 + "\n")
    
    # Example 4: Get weekly movers
    print("4. Weekly Movers (15%+ in 7 days):")
    weekly_movers = query_tool.get_weekly_movers(min_percent=15.0)
    if not weekly_movers.empty:
        print(weekly_movers[['ticker', 'week_start_date', 'week_end_date', 'percent_change']].head(20).to_string(index=False))
    else:
        print("No weekly movers found")
    print("\n" + "="*60 + "\n")
    
    # Example 5: Get price history for a specific stock
    print("5. Price History for AAPL (Last 30 Days):")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    history = query_tool.get_stock_history('AAPL', start_date=thirty_days_ago)
    if not history.empty:
        print(history[['date', 'close_price', 'volume']].tail(10).to_string(index=False))
    else:
        print("No history available")
    print("\n" + "="*60 + "\n")
    
    # Example 6: Custom date range movement
    print("6. Stocks that moved 20%+ between two specific dates:")
    start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    end = datetime.now().strftime('%Y-%m-%d')
    custom_movers = query_tool.get_stocks_by_custom_movement(start, end, min_percent=20.0)
    if not custom_movers.empty:
        print(custom_movers[['ticker', 'start_price', 'end_price', 'percent_change', 'days_elapsed']].head(20).to_string(index=False))
    else:
        print("No stocks moved 20%+ in this date range")
    print("\n" + "="*60 + "\n")
    
    # Example 7: Most volatile stocks
    print("7. Most Volatile Stocks (Last 30 Days - Most frequent big moves):")
    volatile = query_tool.get_most_volatile_stocks(days=30, limit=10)
    if not volatile.empty:
        print("\nTicker  | Times Moved 15%+")
        print("-" * 30)
        for ticker, count in volatile.items():
            print(f"{ticker:8} | {count}")
    else:
        print("No volatile stocks found")
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()
