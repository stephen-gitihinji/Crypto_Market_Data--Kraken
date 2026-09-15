from processing.spark_transformation import transform_crypto_data
from micro_batch_store import write_to_cassandra
from functools import partial

ohlcv_df, spread_df = transform_crypto_data()

print("market metrics df is streaming:", ohlcv_df.isStreaming)
print("spread metrics df is streaming:", spread_df.isStreaming)

ohlcv_write = partial(write_to_cassandra, table_name='market_metrics')
spread_write = partial(write_to_cassandra, table_name='spread_metrics')

ohlcv_query = (
    ohlcv_df.writeStream
    .foreachBatch(ohlcv_write)
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/market_metrics"
    )
    .start())

spread_query = (
    spread_df
    .withColumnRenamed("window", "duration")
    .writeStream
    .outputMode("update")
    .foreachBatch(spread_write)
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/spread_metrics"
    )
    .start())

ohlcv_query.awaitTermination()
spread_query.awaitTermination()