# Stock Market Data Tracker with Supabase

A Python-based system to automatically track daily stock prices, identify significant movers (15%+ daily/weekly), and query historical data. All data is stored in Supabase (PostgreSQL) for easy access and analysis.

## Features

✅ **Automated Daily Data Collection**
- Fetches daily stock prices (open, high, low, close, volume, market cap)
- Updates database automatically after market close
- Handles thousands of tickers with rate limiting

✅ **Movement Detection**
- Identifies stocks that moved 15%+ in a single day
- Tracks weekly movers (15%+ over 7 days)
- Customizable percentage thresholds

✅ **Powerful Querying**
- Query stocks by date range and percentage movement
- Find top gainers and losers
- Track most volatile stocks
- Custom SQL functions for advanced analysis

✅ **Cloud Database (Supabase)**
- Free PostgreSQL hosting
- Real-time updates
- Built-in API access
- Web dashboard for data viewing

## Project Structure

```
stock-tracker/
├── stock_data_loader.py      # Main data collection script
├── query_stocks.py            # Query and analysis tool
├── supabase_schema.sql        # Database schema
├── requirements.txt           # Python dependencies
├── SETUP_GUIDE.md            # Detailed setup instructions
├── README.md                 # This file
├── .env.example              # Environment variables template
├── tickers_example.csv       # Sample ticker list
└── stock_loader.log          # Execution logs (auto-generated)
```

## Quick Start

### 1. Prerequisites
- Python 3.8+
- Supabase account (free)
- CSV file with stock tickers

### 2. Setup Supabase
1. Create account at https://supabase.com
2. Create a new project
3. Copy your Project URL and API Key
4. Run `supabase_schema.sql` in the SQL Editor

### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key-here
TICKERS_CSV=tickers.csv
```

### 5. Load Data
```bash
# First time: Load tickers into database
# (Uncomment line 336 in stock_data_loader.py first)
python stock_data_loader.py

# Daily updates
python stock_data_loader.py
```

### 6. Query Your Data
```bash
python query_stocks.py
```

## Database Schema

### Tables

**`tickers`** - List of all stock tickers to track
- ticker, company_name, sector, active

**`stocks_daily`** - Daily price data for all stocks
- ticker, date, open, high, low, close, volume, market_cap

**`daily_movers`** - Stocks that moved 15%+ in one day
- ticker, date, previous_close, current_close, percent_change, volume

**`weekly_movers`** - Stocks that moved 15%+ in one week
- ticker, week_start_date, week_end_date, percent_change

### Custom Functions

**`get_stocks_by_movement(start_date, end_date, min_percent)`**
- Returns stocks that moved a certain % between any two dates
- Example: `SELECT * FROM get_stocks_by_movement('2024-01-01', '2024-01-31', 15.0);`

## Usage Examples

### Python Queries

```python
from query_stocks import StockQueryTool

query_tool = StockQueryTool(supabase_url, supabase_key)

# Get yesterday's top gainers
gainers = query_tool.get_top_gainers(limit=10)

# Get stocks that moved 20%+ in last 30 days
movers = query_tool.get_daily_movers(min_percent=20.0, start_date='2024-01-01')

# Get price history for specific stock
history = query_tool.get_stock_history('AAPL', start_date='2024-01-01')

# Find stocks that moved 15% between two dates
custom = query_tool.get_stocks_by_custom_movement('2024-01-01', '2024-01-31', 15.0)
```

### SQL Queries

```sql
-- Get all daily movers from last week
SELECT * FROM daily_movers 
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY percent_change DESC;

-- Get stocks with biggest weekly gains
SELECT * FROM weekly_movers 
WHERE percent_change > 0
ORDER BY percent_change DESC
LIMIT 20;

-- Get price history for specific stock
SELECT * FROM stocks_daily 
WHERE ticker = 'AAPL' 
AND date >= '2024-01-01'
ORDER BY date;

-- Use custom function for date range
SELECT * FROM get_stocks_by_movement('2024-01-01', '2024-01-31', 15.0);
```

## Scheduling Daily Updates

### Windows Task Scheduler
1. Open Task Scheduler
2. Create task to run daily at 5:30 PM ET
3. Action: Run `python stock_data_loader.py`

### Linux/Mac Cron
```bash
# Edit crontab
crontab -e

# Add this line (runs at 5:30 PM ET, Mon-Fri)
30 17 * * 1-5 cd /path/to/project && /path/to/venv/bin/python stock_data_loader.py
```

### GitHub Actions (Cloud)
See `SETUP_GUIDE.md` for GitHub Actions configuration

## Data Sources

- **yfinance** - Yahoo Finance API (free, no API key required)
- Provides: Price data, volume, market cap, company info
- Rate limits: Reasonable delays between requests (0.5s default)

## Customization

### Change Movement Threshold
Edit the function calls in `stock_data_loader.py`:
```python
# Default is 15%, change to any value
self.calculate_and_insert_daily_movers(min_percent=20.0)
self.calculate_and_insert_weekly_movers(min_percent=10.0)
```

### Adjust Rate Limiting
Modify the delay between API calls:
```python
loader.run_daily_update(tickers, batch_delay=1.0)  # 1 second between stocks
```

### Add More Tickers
Simply add them to your CSV file and run the loader

## Monitoring & Logs

- Check `stock_loader.log` for detailed execution logs
- Monitor Supabase dashboard for database size and performance
- Set up email alerts in Supabase for errors or limits

## Cost

**Free Tier** (Recommended for most users)
- Supabase: 500MB database, 2GB bandwidth/month
- yfinance API: Free unlimited
- Storage: ~500MB-1GB per year for 5,000 tickers
- **Total: $0/month**

**Paid Options** (For scaling beyond 10,000 tickers)
- Supabase Pro: $25/month (8GB database, 50GB bandwidth)
- Premium APIs: $20-100/month for more reliable data

## Troubleshooting

### No data being collected?
- Markets are only open Mon-Fri (check holidays)
- Verify ticker symbols are correct
- Check `stock_loader.log` for errors

### Rate limiting errors?
- Increase `batch_delay` in the script
- Reduce number of tickers
- Spread updates across multiple hours

### Database connection issues?
- Verify Supabase URL and API key
- Check network connection
- Ensure IP is allowed in Supabase settings

## Future Enhancements

Potential additions for Phase 2:
- [ ] Web dashboard with charts (Streamlit)
- [ ] Email/SMS alerts for big movers
- [ ] Technical indicators (RSI, MA, MACD)
- [ ] Backtesting capabilities
- [ ] Options data tracking
- [ ] News sentiment analysis
- [ ] Portfolio tracking

## Contributing

Feel free to fork this project and submit pull requests with improvements!

## License

MIT License - Use freely for personal or commercial projects

## Support

For issues:
1. Check `stock_loader.log` for errors
2. Review `SETUP_GUIDE.md` for detailed instructions
3. Verify Supabase connection and tables
4. Ensure Python dependencies are installed

## Acknowledgments

- [yfinance](https://github.com/ranaroussi/yfinance) for free stock data
- [Supabase](https://supabase.com) for free PostgreSQL hosting
- Yahoo Finance for market data

- 
