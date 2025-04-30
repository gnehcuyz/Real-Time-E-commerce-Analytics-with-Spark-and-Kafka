# Real-Time E-commerce Analytics with Kafka and PySpark

This project simulates and processes real-time e-commerce user interaction data (e.g., product views, clicks, add-to-cart) using Apache Kafka, PySpark Structured Streaming, and optional AWS storage.

It provides a basic framework to:
- Simulate user events in real time
- Ingest those events using Kafka
- Process and aggregate them using PySpark
- Store the processed output in a data lake
Visualize insights with Superset or Tableau

## Architecture Design
I will update this section with a diagram later.

## Dataset

This project uses the [Retailrocket dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) from Kaggle, which contains real-world anonymized e-commerce event data collected from a retail website. The dataset includes user interactions such as product views, add-to-carts, and transactions.

We mainly use the `events.csv` file to simulate real-time user activities. Each event includes:
- `timestamp`: Time when the event occurred
- `visitorid`: Unique identifier for the user
- `event`: Type of event (view, addtocart, transaction)
- `itemid`: ID of the item interacted with
- `transactionid` (optional): Present only for transaction events

These events are streamed into Kafka topics to mimic real-time behavior and are then processed using PySpark for analytics and dashboarding.


[//]: # (## Tech Stack)

[//]: # ()
[//]: # (- Apache Kafka – Real-time message streaming)

[//]: # (- Apache Zookeeper – Kafka coordination)

[//]: # (- PySpark Structured Streaming – Real-time processing)

[//]: # (- Docker Compose – Local Kafka/Zookeeper setup)

[//]: # (- Python – Kafka producer to simulate e-commerce events)

[//]: # (- Postgres – Data sink)

[//]: # (- Apache Superset – BI Dashboard)

[//]: # ()
[//]: # (## Project Structure)

[//]: # (```)

[//]: # (real-time-ecommerce-analytics/)

[//]: # (├── data_generator/               # Extract: Simulates user clickstream data)

[//]: # (│   └── producer.py)

[//]: # (├── spark_streaming/              # Transform: PySpark Structured Streaming jobs)

[//]: # (│   ├── streaming_job.py)

[//]: # (│   └── transformations.py        )

[//]: # (├── data_sink/                    # Load: Output handling)

[//]: # (│   └── write_to_postgres.py      )

[//]: # (├── docker/                       # Docker setup)

[//]: # (│   └── docker-compose.yml)

[//]: # (├── config/                       # Kafka/Spark config &#40;topics, schema, env vars&#41;)

[//]: # (│   └── settings.json)

[//]: # (├── requirements.txt)

[//]: # (├── .gitignore)

[//]: # (└── README.md)

[//]: # (```)

[//]: # ()
[//]: # (## How to Run)

[//]: # ()
[//]: # (### 1. Start Kafka & Zookeeper)

[//]: # ()
[//]: # (    cd docker)

[//]: # (    docker-compose up -d)

[//]: # ()
[//]: # (### 2. Simulate User Events)

[//]: # ()
[//]: # (    cd data_generator)

[//]: # (    python producer.py)

[//]: # ()
[//]: # (### 3. Run Spark Streaming Job)

[//]: # ()
[//]: # (    cd spark_streaming)

[//]: # (    spark-submit streaming_job.py)

[//]: # ()
[//]: # (## Example Events)

[//]: # ()
[//]: # (Sample JSON messages sent to Kafka:)

[//]: # ()
[//]: # ({)

[//]: # (  "user_id": "u123",)

[//]: # (  "event_type": "view_product",)

[//]: # (  "product_id": "p456",)

[//]: # (  "timestamp": "2025-04-30T17:00:00Z")

[//]: # (})

[//]: # ()
[//]: # (## Possible Analytics)

[//]: # ()
[//]: # (- Most viewed products &#40;real-time&#41;)

[//]: # (- Click-through rate per product)

[//]: # (- Session length per user)

[//]: # (- Conversion funnel &#40;views → add-to-cart → purchase&#41;)

[//]: # ()
[//]: # (## Future Work)

[//]: # ()
[//]: # (- Integrate with AWS S3 or RDS)

[//]: # (- Train ML models on session behavior)

[//]: # (- Add REST API for real-time metrics)

[//]: # (- Visualize in Superset or Tableau)

[//]: # ()
[//]: # (## License)

[//]: # ()
[//]: # (MIT License)

[//]: # ()
[//]: # (> Built for learning and demonstration purposes. Not production-hardened.)
