-- Supabase Database Schema for Stock Data Tracker
-- Run this SQL in your Supabase SQL Editor to create all tables

-- 1. Tickers table - stores list of all tickers to track
CREATE TABLE IF NOT EXISTS tickers (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Stocks Daily table - main table with daily price data
CREATE TABLE IF NOT EXISTS stocks_daily (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    date DATE NOT NULL,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2) NOT NULL,
    volume BIGINT,
    market_cap BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- 3. Daily Movers table - stocks that moved 15%+ in one day
CREATE TABLE IF NOT EXISTS daily_movers (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    previous_close DECIMAL(10,2) NOT NULL,
    current_close DECIMAL(10,2) NOT NULL,
    percent_change DECIMAL(8,2) NOT NULL,
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- 4. Weekly Movers table - stocks that moved 15%+ in one week
CREATE TABLE IF NOT EXISTS weekly_movers (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    week_start_close DECIMAL(10,2) NOT NULL,
    week_end_close DECIMAL(10,2) NOT NULL,
    percent_change DECIMAL(8,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticker, week_end_date)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stocks_daily_ticker ON stocks_daily(ticker);
CREATE INDEX IF NOT EXISTS idx_stocks_daily_date ON stocks_daily(date);
CREATE INDEX IF NOT EXISTS idx_stocks_daily_ticker_date ON stocks_daily(ticker, date);

CREATE INDEX IF NOT EXISTS idx_daily_movers_ticker ON daily_movers(ticker);
CREATE INDEX IF NOT EXISTS idx_daily_movers_date ON daily_movers(date);
CREATE INDEX IF NOT EXISTS idx_daily_movers_percent ON daily_movers(percent_change);

CREATE INDEX IF NOT EXISTS idx_weekly_movers_ticker ON weekly_movers(ticker);
CREATE INDEX IF NOT EXISTS idx_weekly_movers_end_date ON weekly_movers(week_end_date);
CREATE INDEX IF NOT EXISTS idx_weekly_movers_percent ON weekly_movers(percent_change);

CREATE INDEX IF NOT EXISTS idx_tickers_active ON tickers(active);

-- Add foreign key constraints (optional, but recommended for data integrity)
ALTER TABLE stocks_daily 
    ADD CONSTRAINT fk_stocks_ticker 
    FOREIGN KEY (ticker) 
    REFERENCES tickers(ticker) 
    ON DELETE CASCADE;

ALTER TABLE daily_movers 
    ADD CONSTRAINT fk_daily_movers_ticker 
    FOREIGN KEY (ticker) 
    REFERENCES tickers(ticker) 
    ON DELETE CASCADE;

ALTER TABLE weekly_movers 
    ADD CONSTRAINT fk_weekly_movers_ticker 
    FOREIGN KEY (ticker) 
    REFERENCES tickers(ticker) 
    ON DELETE CASCADE;

-- Create updated_at trigger function for tickers table
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add trigger to tickers table
CREATE TRIGGER update_tickers_updated_at
    BEFORE UPDATE ON tickers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for easy querying of stocks with percentage changes
CREATE OR REPLACE VIEW stocks_with_changes AS
SELECT 
    s1.ticker,
    s1.company_name,
    s1.date,
    s1.close_price,
    s1.volume,
    s1.market_cap,
    s2.close_price as previous_close,
    ROUND(((s1.close_price - s2.close_price) / s2.close_price * 100)::numeric, 2) as daily_percent_change
FROM stocks_daily s1
LEFT JOIN stocks_daily s2 
    ON s1.ticker = s2.ticker 
    AND s2.date = (
        SELECT MAX(date) 
        FROM stocks_daily 
        WHERE ticker = s1.ticker 
        AND date < s1.date
    )
ORDER BY s1.date DESC, s1.ticker;

-- Create a function to get stocks by date range and percentage movement
CREATE OR REPLACE FUNCTION get_stocks_by_movement(
    start_date DATE,
    end_date DATE,
    min_percent DECIMAL DEFAULT 15.0
)
RETURNS TABLE (
    ticker VARCHAR,
    company_name VARCHAR,
    start_price DECIMAL,
    end_price DECIMAL,
    percent_change DECIMAL,
    days_elapsed INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH stock_prices AS (
        SELECT 
            sd1.ticker,
            sd1.company_name,
            sd1.close_price as end_price,
            sd1.date as end_date,
            sd2.close_price as start_price,
            sd2.date as start_date
        FROM stocks_daily sd1
        INNER JOIN stocks_daily sd2 
            ON sd1.ticker = sd2.ticker
        WHERE sd1.date = end_date
            AND sd2.date = start_date
    )
    SELECT 
        sp.ticker,
        sp.company_name,
        sp.start_price,
        sp.end_price,
        ROUND(((sp.end_price - sp.start_price) / sp.start_price * 100)::numeric, 2) as percent_change,
        (sp.end_date - sp.start_date)::INTEGER as days_elapsed
    FROM stock_prices sp
    WHERE ABS((sp.end_price - sp.start_price) / sp.start_price * 100) >= min_percent
    ORDER BY ABS((sp.end_price - sp.start_price) / sp.start_price * 100) DESC;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM get_stocks_by_movement('2024-01-01', '2024-01-31', 15.0);

COMMENT ON TABLE tickers IS 'List of stock tickers to track';
COMMENT ON TABLE stocks_daily IS 'Daily stock price data for all tracked tickers';
COMMENT ON TABLE daily_movers IS 'Stocks that moved 15% or more in a single day';
COMMENT ON TABLE weekly_movers IS 'Stocks that moved 15% or more in a week';
COMMENT ON FUNCTION get_stocks_by_movement IS 'Query stocks that moved a certain percentage between two dates';
