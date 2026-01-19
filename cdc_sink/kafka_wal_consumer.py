import os
from dotenv import load_dotenv
from confluent_kafka import Consumer


class KafkaWALConsumer:
    def __init__(self, cluster_config: str, topic: str):
        self.cluster_config = cluster_config
        self.topic = topic
        self.consumer = None

    def __enter__(self):
        self.consumer = Consumer(self.cluster_config)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.consumer:
            self.consumer.close()

    def consume(self, callback) -> None:
        self.consumer.subscribe(self.topic)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    print("\rWaiting...", end="", flush=True)
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    callback(msg)
        except KeyboardInterrupt:
            print("Stopping consumption...")


if __name__ == "__main__":
    load_dotenv()

    config = {
        "bootstrap.servers": os.getenv("KAFKA_CLUSTER_BOOTSTRAP_SERVERS"),
        "sasl.username": os.getenv("KAFKA_CLUSTER_API_KEY"),
        "sasl.password": os.getenv("KAFKA_CLUSTER_API_SECRET"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }
