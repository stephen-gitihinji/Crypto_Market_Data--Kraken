from processing.spark_schemas import ohlcv_schema, recent_spreads_schema
from processing.spark_session import spark
from streaming.consumers.spark_consumer import consume_topic
from pyspark.sql import functions as f
from processing.price_metrics import price_metrics
from processing.volatility_metrics import volatility_metrics

spark = spark
def transform_crypto_data():
    #extract data from kafka topics
    ohlcv_df = consume_topic(spark, "postgres-ohlcv_crypto_data")
    recent_spreads_df = consume_topic(spark, "postgres-recent_crypto_spreads")

    #deserialize the data
    ohlcv_df = ohlcv_df.select(f.from_json(f.col('value').cast('string'), schema=ohlcv_schema).alias('data')).select('data.*')
    recent_spreads_df = recent_spreads_df.select(f.from_json(f.col('value').cast('string'), schema=recent_spreads_schema).alias('data')).select('data.*')

    #transform the data
    ohlcv_df = ohlcv_df.withColumn('timestamp', f.col('timestamp').cast('timestamp'))
    recent_spreads_df = (recent_spreads_df.withColumnRenamed('time', 'timestamp')
                        .withColumn('timestamp',f.col('timestamp').cast('timestamp')))

    ohlcv_df, spread_df = price_metrics(ohlcv_df, recent_spreads_df)
    ohlcv_df = volatility_metrics(ohlcv_df)
    return ohlcv_df, spread_df