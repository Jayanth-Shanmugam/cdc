#!/usr/bin/env python

import os
from confluent_kafka import Consumer
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv("KAFKA_CLUSTER_BOOTSTRAP_SERVERS"),
        'sasl.username':     os.getenv('KAFKA_CLUSTER_API_KEY'),
        'sasl.password':     os.getenv("KAFKA_CLUSTER_API_SECRET"),

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'group.id':          'kafka-python-getting-started',
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "postgres_cdc"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("\rWaiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()