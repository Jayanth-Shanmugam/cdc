import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer

class KafkaWALProducer:
    def __init__(self, cluster_config, topic):
        self.cluster_config = cluster_config
        self.topic = topic
        self.wal_producer = None
    

    def __enter__(self):
        self.wal_producer = Producer(self.cluster_config)

        return self
    

    def __exit__(self):
        if self.wal_producer:
            self.wal_producer.flush()
    

    def delivery_callback(self, err, msg):
        if err:
            print(f'ERROR: Message failed delivery: {err}')
        else:
            print(
            "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), 
                    key=msg.key().decode('utf-8'), 
                    value=msg.value().decode('utf-8')
                )
            )


    def produce(self, key: str, message: dict):
        self.wal_producer.produce(
            self.topic, 
            key,
            json.dumps(message),
            callback=self.delivery_callback
        )


if __name__ == "__main__":
    load_dotenv()

    cluster_config = {
        'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
        'sasl.username':     os.getenv("CLUSTER_API_KEY"),
        'sasl.password':     os.getenv("CLUSTER_API_SECRET"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    
