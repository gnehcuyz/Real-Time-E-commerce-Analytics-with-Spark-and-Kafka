import pandas as pd
from kafka import KafkaProducer
import json
import time
from tqdm import tqdm
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_SERVER, KAFKA_TOPIC, CSV_PATH

class EventProducer:
    """
    A class to produce and send e-commerce events to a Kafka topic.
    This class reads event data from a CSV file, formats it into a structured JSON format,
    and sends it to a specified Kafka topic using the KafkaProducer.
    """

    def __init__(self, kafka_server=KAFKA_SERVER, topic=KAFKA_TOPIC):
        """
        Initialization

        :param kafka_server: The Kafka server address
        :param topic: The Kafka topic to which events will be sent
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def format_event(self, row):
        """
        Format the event before sending to the Kafka topic

        :param row: A pandas Series object representing a row of data from the CSV file.
        :return: formatted data
        """
        return {
            "timestamp": row["timestamp"],
            "visitorid": row["visitorid"],
            "event": row["event"],
            "itemid": row["itemid"],
            "transactionid": row.get("transactionid", None)
        }

    def send_events(self, path=CSV_PATH, limit=500, delay=0.1):
        """
        Reads events from a CSV file and sends them to the Kafka topic.

        :param path: Path to the CSV file containing event data.
        :param limit: Maximum number of events to send (default is 5).
        :param delay: Delay in seconds between sending each event (default is 0.1).
        """
        df = pd.read_csv(path)
        for _, row in tqdm(df.head(limit).iterrows(), total=limit, desc="Sending events"):
            event = self.format_event(row)
            self.producer.send(self.topic, event)
            # print(f"Sent: {event}")
            time.sleep(delay)
        self.producer.flush()