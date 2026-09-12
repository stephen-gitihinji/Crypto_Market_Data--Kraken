from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType

ohlcv_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume_weighted_avg", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("trade_count", IntegerType(), True),
    StructField("pair", StringType(), True)
])

l2_book_schema = StructType([
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("side", StringType(), True),
    StructField("pair", StringType(), True),
    StructField("timestamp", LongType(), True)
])

recent_trades_schema = StructType([
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("time", LongType(), True),
    StructField("side", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("misc", StringType(), True),
    StructField("trade_id", IntegerType(), True),
    StructField("pair", StringType(), True)
])

recent_spreads_schema = StructType([
    StructField("time", LongType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("pair", StringType(), True)
])