import argparse
from producer import EventProducer
from consumer import EventConsumer


def run_producer():
    producer = EventProducer()
    producer.send_events()

def run_consumer():
    consumer = EventConsumer()
    consumer.listen()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Kafka producer or consumer.")
    parser.add_argument("mode", choices=["producer", "consumer"], help="Choose which mode to run.")
    args = parser.parse_args()

    if args.mode == "producer":
        run_producer()
    elif args.mode == "consumer":
        run_consumer()
