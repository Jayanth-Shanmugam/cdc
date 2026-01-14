import json

import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection


class Producer:
    def __init__(
        self, host: str, port: int, username: str, password: str, queue_name: str
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def __enter__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=pika.PlainCredentials(self.username, self.password),
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def publish(self, message: dict):
        self.channel.basic_publish(
            exchange="", routing_key=self.queue_name, body=json.dumps(message)
        )


class Consumer:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        producer: Producer,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.producer = producer

    def consume(self, slot_name: str, options: dict, decode: bool) -> dict:
        """
        Core consumer method. Connects to the database through a
        logical replication connection, waits for incoming WAL
        changes and publishes them to a RabbitMQ queue.

        Args:
            slot_name: Name of the replication slot
            options: Optional parameters for consuming changes
            decode: Decode flag for the slot
        Returns:
            None
        """

        conn = psycopg2.connect(
            dbname=self.database,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            connection_factory=LogicalReplicationConnection,
        )

        cur = conn.cursor()

        cur.start_replication(
            slot_name=slot_name,
            options=options,
            decode=decode,
        )

        while True:
            msg = cur.read_message()
            try:
                if msg:
                    msg_payload = json.loads(msg.payload)
                    db_changes = msg_payload.get("change")
                    for change in db_changes:
                        self.producer.publish(change)
                else:
                    print("\rListening for WAL changes...", end="", flush=True)
            except KeyboardInterrupt:
                print("Closing replication connection...")
