-- create keyspace
CREATE KEYSPACE IF NOT EXISTS crypto WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};

-- Create market_metrics table
CREATE TABLE IF NOT EXISTS crypto.market_metrics(
    timestamp timestamp,
    open float,
    high float,
    low float,
    close float,
    volume_weighted_avg float,
    volume float,
    trade_count int,
    pair text,
    price_change float,
    price_change_pct float,
    typical_price float,
    normalized_price_position float,
    intraday_return float,
    high_low_range float,
    relative_volatility float,
    PRIMARY KEY (pair, timestamp)
) WITH CLUSTERING ORDER BY(timestamp DESC);

-- create spread_metrics table
CREATE TABLE IF NOT EXISTS spread_metrics(
    duration text,
    pair text,
    avg_bid_price float,
    avg_ask_price float,
    avg_spread float,
    PRIMARY KEY (pair, duration)
);