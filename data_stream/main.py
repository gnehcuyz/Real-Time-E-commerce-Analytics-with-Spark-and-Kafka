from pyspark.sql import SparkSession
from kafka_stream_reader import KafkaStreamReader
from transformations import apply_transformations
from config.kafka_config import KAFKA_TOPIC, CHECKPOINT_LOCATION, KAFKA_SERVER


def log_batch_size(batch_df, batch_id):
    count = batch_df.count()
    print(f"Batch {batch_id} has {count} rows")

def run_stream_pipeline():
    spark = SparkSession.builder \
        .appName("EcommerceStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    reader = KafkaStreamReader(spark, KAFKA_TOPIC, KAFKA_SERVER)
    kafka_df = reader.read_stream()
    transformed_df = apply_transformations(kafka_df)

    query = transformed_df.writeStream \
        .foreachBatch(log_batch_size) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    # .format("console") \
    # .outputMode("append") \
    query.awaitTermination()


if __name__ == "__main__":
    run_stream_pipeline()