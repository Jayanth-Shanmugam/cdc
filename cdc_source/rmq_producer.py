import json
import pika

class Producer:
    def __init__(
        self, 
        host: str = "localhost", 
        port: int = 5672,
        vhost: str = '/', 
        username: str = "guest", 
        password: str = "guest", 
        queue_name: str = "",
    ):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def __enter__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                self.host,
                self.port,
                self.vhost,
                pika.PlainCredentials(self.username, self.password)
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection and self.channel and not self.channel.is_closed:
            self.channel.close()
            self.connection.close()

    def publish(self, message: dict):
        self.channel.basic_publish(
            exchange="", 
            routing_key=self.queue_name, 
            body=json.dumps(message)
        )