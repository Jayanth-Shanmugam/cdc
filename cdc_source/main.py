# main.py
import os
import argparse
from dotenv import load_dotenv
from pg_wal_consumer import PGWALConsumer
from kafka_wal_producer import KafkaWALProducer


def main():
    load_dotenv()

    POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    KAFKA_CLUSTER_BOOTSTRAP_SERVERS = os.getenv("KAFKA_CLUSTER_BOOTSTRAP_SERVERS")
    KAFKA_CLUSTER_API_KEY = os.getenv("KAFKA_CLUSTER_API_KEY")
    KAFKA_CLUSTER_API_SECRET = os.getenv("KAFKA_CLUSTER_API_SECRET")

    parser = argparse.ArgumentParser()

    # PostgreSQL database arguments
    parser.add_argument("--pg-host", type=str, default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", required=True)
    parser.add_argument("--slot-name", required=True)

    # Kafka cluster arguments
    parser.add_argument("--kafka-topic", type=str, required=True)

    args = parser.parse_args()

    reader = PGWALConsumer(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_db,
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
    )

    kafka_cluster_config = {
        "bootstrap.servers": KAFKA_CLUSTER_BOOTSTRAP_SERVERS,
        "sasl.username": KAFKA_CLUSTER_API_KEY,
        "sasl.password": KAFKA_CLUSTER_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "acks": "all",
    }

    with KafkaWALProducer(
        cluster_config=kafka_cluster_config, topic=args.kafka_topic
    ) as producer:
        reader.consume(
            slot_name=args.slot_name,
            options={},
            decode=True,
            on_change=producer.produce,
        )


if __name__ == "__main__":
    main()
