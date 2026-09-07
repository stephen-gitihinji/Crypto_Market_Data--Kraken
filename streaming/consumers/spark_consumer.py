from pandas import col
from pyspark.sql import SparkSession, functions as F
from config import KAFKA_BROKER

spark = SparkSession.builder.appName("SparkConsumer").getOrCreate()

#For console output to reduce verbose
spark.sparkContext.setLogLevel("ERROR")

def consume_topic(topic_name, df_schema):
    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option('subscribe', topic_name)
        .option('startingOffsets', 'earliest')
        .load()
    )
    #casting and deserializing the data from kafka
    df = df.select(F.from_json(F.col("value").cast("string"), schema=df_schema).alias("data")).select("data.*")
    return df