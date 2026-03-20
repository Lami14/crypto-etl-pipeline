-- Create the database schema for storing crypto price data

CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    coin_name VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    current_price_usd NUMERIC(20, 8) NOT NULL,
    market_cap NUMERIC(30, 2),
    total_volume NUMERIC(30, 2),
    price_change_24h NUMERIC(20, 8),
    price_change_percentage_24h NUMERIC(10, 4),
    high_24h NUMERIC(20, 8),
    low_24h NUMERIC(20, 8),
    circulating_supply NUMERIC(30, 2),
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast queries by coin and time
CREATE INDEX IF NOT EXISTS idx_coin_id ON crypto_prices(coin_id);
CREATE INDEX IF NOT EXISTS idx_fetched_at ON crypto_prices(fetched_at);

-- View: latest price per coin
CREATE OR REPLACE VIEW latest_crypto_prices AS
SELECT DISTINCT ON (coin_id)
    coin_id,
    coin_name,
    symbol,
    current_price_usd,
    price_change_percentage_24h,
    market_cap,
    fetched_at
FROM crypto_prices
ORDER BY coin_id, fetched_at DESC;

