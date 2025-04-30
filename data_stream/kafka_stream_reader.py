class KafkaStreamReader:
    def __init__(self, spark, topic, bootstrap_servers):
        self.spark = spark
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def read_stream(self):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )
