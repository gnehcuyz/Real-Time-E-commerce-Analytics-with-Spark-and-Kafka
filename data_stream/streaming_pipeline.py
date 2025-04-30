from pyspark.sql import SparkSession
from config.kafka_config import KAFKA_TOPIC, CHECKPOINT_LOCATION, KAFKA_SERVER
from kafka_stream_reader import KafkaStreamReader
from transformations import apply_transformations
from data_sink.writer import write_events


def log_batch_to_postgres(batch_df, batch_id):
    """
    Process each micro-batch and write to Postgres.

    :param batch_df: The DataFrame representing the current micro-batch.
    :param batch_id: The unique identifier for the current micro-batch.
    """
    records = batch_df.toPandas().to_dict(orient="records")
    if records:
        print(f"[Batch {batch_id}] Writing {len(records)} events to Postgres")
        write_events(records)


def run_streaming_pipeline():
    """
    Main function to run the streaming pipeline.
    """
    spark = SparkSession.builder \
        .appName("EcommerceStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    reader = KafkaStreamReader(spark, KAFKA_TOPIC, KAFKA_SERVER)
    kafka_df = reader.read_stream()

    transformed_df = apply_transformations(kafka_df)

    query = transformed_df.writeStream \
        .foreachBatch(log_batch_to_postgres) \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()
