import json
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_SERVER, KAFKA_TOPIC, CHECKPOINT_LOCATION

class KafkaStreamReader:
    def __init__(self, spark, topic=KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER):
        """

        :param spark: The Spark session used for reading the Kafka stream.
        :param topic: The Kafka topic to subscribe to for streaming data.
        :param bootstrap_servers: The Kafka server addresses for connecting to the Kafka cluster.
        """
        self.spark = spark
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def read_stream(self):
        """
        Configures Spark readStream to consume new messages from the specified Kafka topic and servers.
        :return: A PySpark DataFrame representing the streaming data from Kafka.
        """
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .option("failOnDataLoss", "true")
            .load()
        )
