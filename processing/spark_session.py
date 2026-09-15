from pyspark.sql import SparkSession
from config import CASSANDRA_HOST, CASSANDRA_PORT

#spark session configuration + spark-cassandra configurations
spark = (SparkSession.builder.appName("SparkConsumer")
         .config("spark.cassandra.connection.host", CASSANDRA_HOST)
         .config("spark.cassandra.connection.port", CASSANDRA_PORT)
         .getOrCreate())

#For console output - to reduce verbose text
spark.sparkContext.setLogLevel("ERROR")