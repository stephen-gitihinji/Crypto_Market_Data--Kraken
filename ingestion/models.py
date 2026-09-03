from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from time import time

#using sqlalchemy core to define the table(s) structure

metadata = MetaData()

OhlcvCryptoData = Table(
    "ohlcv_crypto_data",
    metadata,
    Column("timestamp", Integer, nullable=False),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume_weighted_avg", Float),
    Column("volume", Float),
    Column("trade_count", Integer),
    Column("pair", String)
)

L2BookData = Table(
    "l2_book_data",
    metadata,
    Column("price", Float),
    Column("volume", Float),
    Column("side", String),
    Column("pair", String),
    Column("timestamp", Integer, nullable=False)
)

GroupedBookData = Table(
    "grouped_book_data",
    metadata,
    Column("price", Float),
    Column("qty", Float),
    Column("side", String),
    Column("pair", String),
    # Column("updated_at", Integer, nullable=False, default=lambda:int(time()))
)

RecentCryptoTrades = Table(
    "recent_crypto_trades",
    metadata,
    Column("price", Float),
    Column("volume", Float),
    Column("time", Integer, nullable=False),
    Column("side", String),
    Column("order_type", String),
    Column("misc", String),
    Column("trade_id", Integer),
    Column("pair", String)
)

RecentCryptoSpreads = Table(
    "recent_crypto_spreads",
    metadata,
    Column("time", Integer, nullable=False),
    Column("bid", Float),
    Column("ask", Float),
    Column("pair", String)
)