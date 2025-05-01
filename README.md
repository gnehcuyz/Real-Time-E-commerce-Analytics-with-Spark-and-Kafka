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


## Tech Stack
- Python
- Docker Compose – Runs all services locally in isolated, reproducible environments.
- PostgreSQL – Stores cleaned event data for querying and long-term analysis.
- Apache Kafka – Streams user activity events between components in real time.
- Apache ZooKeeper – Manages Kafka broker coordination and system metadata.
- Apache Spark Structured Streaming – Processes and transforms streaming data on the fly.
- Apache Superset – Visualizes processed data through real-time interactive dashboards.

## Running Instructions

### 1. Start Docker Services

From the root directory (where `docker-compose.yml` is):

```bash
docker compose build
docker compose up -d
```

This will start:
- Kafka
- Zookeeper
- PostgreSQL
- Superset

---

### 2. Initialize Superset (first time only)

Upgrade the database schema (applies migrations)
```bash
docker exec -it <container_name> superset db upgrade
```
Create the first admin user:
```bash
docker exec -it <container_name> superset fab create-admin 
```
Initialize Superset
```bash
docker exec -it <container_name> superset init
```

Then visit: [http://localhost:8088](http://localhost:8088)

Log in with your credentials.

---

### 3. Run the Kafka Producer (Simulate User Events)
In a separate terminal, navigate to the `data_stream` directory:
```bash
python data_stream/main.py
```

---

### 4. Run the PySpark Streaming Job

In a separate terminal, navigate to the `data_stream` directory:

```bash
python data_stream/streaming_pipeline.py
```

This consumes events from Kafka and writes them into PostgreSQL.

---

### 5. Connect Superset to PostgreSQL

In Superset UI:

1. Go to **Data** → **Databases** → **+ Database**
2. Select **PostgreSQL**
3. Fill in the credentials:

```
Host: host.docker.internal
Port: 5433
Database name: <your-DB-name>
Username: <your-DB-username>
Password: <your-DB-password>
```

Click **Connect**.

---

### 6. Create Dataset

1. Go to **Datasets** → **+ Dataset**
2. Select:

- Database: `ecommerce`
- Schema: `public`
- Table: `events`

Click **Add**.

---

### 7. Build Charts & Dashboards

Example charts:

- **Line chart**: Event volume over time (`COUNT(*)` on `timestamp`)
- **Bar chart**: Top 10 items by `purchase` event count
- **Pie chart**: Distribution of `event_type`

---

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
