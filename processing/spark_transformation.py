from processing.spark_schemas import ohlcv_schema, recent_spreads_schema
from streaming.consumers.spark_consumer import consume_topic
from pyspark.sql import functions as f
from processing.price_metrics import price_metrics
from processing.volatility_metrics import volatility_metrics

#consuming the kafka topics
ohlcv_df = consume_topic("postgres-ohlcv_crypto_data", ohlcv_schema)
recent_spreads_df = consume_topic("postgres-recent_crypto_spreads", recent_spreads_schema)

ohlcv_df = ohlcv_df.withColumn('timestamp', f.col('timestamp').cast('timestamp'))
recent_spreads_df = (recent_spreads_df.withColumnRenamed('time', 'timestamp')
                     .withColumn('timestamp',f.col('timestamp').cast('timestamp')))

#final transformed dataframes
ohlcv_df, spread_df = price_metrics(ohlcv_df, recent_spreads_df)
ohlcv_df = volatility_metrics(ohlcv_df)