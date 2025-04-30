from kafka import KafkaConsumer
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_SERVER, KAFKA_TOPIC, CSV_PATH

class EventConsumer:
    def __init__(self, topic=KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER):
        """
        Initialization
        :param topic: The Kafka topic to subscribe to
        :param bootstrap_servers: The Kafka server address
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

    def listen(self):
        print(f"Listening to topic: {self.topic}")
        for message in self.consumer:
            print("Received:", message.value)