from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType

def apply_transformations(raw_df):
    """
    Parses Kafka JSON messages using a defined schema and extracts relevant fields into a clean DataFrame.
    :param raw_df: A PySpark DataFrame containing raw Kafka messages.
    :return: A PySpark DataFrame with parsed and structured event data.
    """
    schema = StructType() \
        .add("timestamp", LongType()) \
        .add("visitorid", LongType()) \
        .add("event", StringType()) \
        .add("itemid", LongType()) \
        .add("transactionid", StringType())

    json_df = raw_df.selectExpr("CAST(value AS STRING)")
    parsed_df = json_df.select(from_json(col("value"), schema).alias("data"))
    final_df = parsed_df.select("data.*")

    return final_df