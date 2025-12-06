from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Create a Spark session
spark = (SparkSession.builder.appName("Kafka-Spark-Structured-Streaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1")
    .getOrCreate())

# Read streaming data from Kafka topic 'sensor-data'
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor-data")
    .load()
)

# Define the schema for the sensor data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("temperature", DoubleType()) \
    
# Parse JSON
json_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# Output
query = (
    json_df.writeStream
        .outputMode("append")
        .format("console")
        .start()
)

# Await termination
query.awaitTermination()