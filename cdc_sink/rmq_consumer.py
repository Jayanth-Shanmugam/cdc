import json
import pika, sys, os

class Consumer:
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

    def consume(self, callback):
        self.channel.basic_consume(
            queue = self.queue_name,
            on_message_callback= callback,
            auto_ack = True
        )

        self.channel.start_consuming()

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='cdc_stream')

    def callback(ch, method, properties, body):
        change = json.loads(body)
        print(f" [x] Received {change}")

    channel.basic_consume(queue='cdc_stream', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)