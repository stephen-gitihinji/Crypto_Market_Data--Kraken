from config import KAFKA_BROKER

def consume_topic(spark, topic_name):
    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option('subscribe', topic_name)
        .option('startingOffsets', 'earliest')
        .load()
    )
    return df