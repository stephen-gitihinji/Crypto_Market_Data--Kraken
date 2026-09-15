import os
from dotenv import load_dotenv

load_dotenv()

#postgres connection configuration
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

#kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
OHLCV_TOPIC = os.getenv("OHLCV_TOPIC")
L2_BOOK_TOPIC = os.getenv("L2_BOOK_TOPIC")
GROUPED_BOOK_TOPIC = os.getenv("GROUPED_BOOK_TOPIC")
RECENT_CRYPTO_TRADES_TOPIC = os.getenv("RECENT_CRYPTO_TRADES_TOPIC")
RECENT_CRYPTO_SPREADS_TOPIC = os.getenv("RECENT_CRYPTO_SPREADS_TOPIC")

#cassndra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")
