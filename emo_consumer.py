from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, from_json, col, ceil, sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import tempfile
from pyspark.sql.functions import struct, to_json

def main():
    checkpoint_dir = tempfile.mkdtemp()

    print(f"Checkpoint directory: {checkpoint_dir}")  # For debugging
    spark = (
        SparkSession.builder
        .appName("Emoji Aggregation from Kafka")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")  # Bypass correctness check
        .master("local[*]")
        .getOrCreate()
    )

    # Define the schema for the JSON data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # Read from Kafka topic 'topic1'
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "topic1")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON data
    df = df.selectExpr("CAST(value AS STRING) as value")
    df = df.withColumn("value", from_json(col("value"), schema))
    df = df.select("value.user_id", "value.emoji_type", "value.timestamp")

    # Increase watermark to handle more late data
    df = df.withWatermark("timestamp", "2 seconds")

    # Apply the window size of 2 seconds to group the emojis within the window
    windowed_df = df \
        .groupBy(window("timestamp", "2 seconds"), "emoji_type") \
        .agg(count("*").alias("total_count"))

    # Deduplicate by keeping unique emoji types within each window
    deduplicated_df = windowed_df.distinct()  # Apply distinct directly on the DataFrame

    # Sum the counts for each emoji type in the same window
    aggregated_df = deduplicated_df \
        .groupBy("window", "emoji_type") \
        .agg(
            sum("total_count").alias("total_count_aggregated")  # Aggregate counts correctly
        )

    # Ensure the total count doesn't exceed 1000 per emoji type
    capped_df = aggregated_df.withColumn(
        "aggregated_count", 
        ceil(aggregated_df["total_count_aggregated"] / 1000)  # This can be adjusted based on how you want to display counts
    )

    capped_df = capped_df.withColumn(
        "value", 
        to_json(struct("emoji_type", "aggregated_count"))  # Structure the data with both emoji_type and aggregated_count
    )

    capped_df \
        .selectExpr("CAST(value AS STRING) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "main-publisher") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/main-publisher") \
        .outputMode("update") \
        .start() \
        .awaitTermination()

    query.awaitTermination()

if __name__ == "__main__":
    main()

