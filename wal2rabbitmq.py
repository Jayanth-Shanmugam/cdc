import json
import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

class Consumer:
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
    
    def consume(self, slot_name: str, options: dict, decode: bool) -> dict:
        '''
        Core consumer method. Connects to the database through a 
        logical replication connection, waits for incoming WAL
        changes and publishes them to a RabbitMQ queue.

        Args:
            slot_name: Name of the replication slot
            options: Optional parameters for consuming changes
            decode: Decode flag for the slot
        Returns:
            None
        '''

        conn = psycopg2.connect(
            dbname = self.dbname,
            user = self.user,
            password = self.password,
            host = self.host,
            port = self.port,
            connection_factory = LogicalReplicationConnection
        )

        cur = conn.cursor()

        cur.start_replication(
            slot_name = slot_name,
            options = options,
            decode = decode,
        )

        while True:
            msg = cur.read_message()
            try:
                if msg:
                    msg_payload = json.loads(msg.payload)
                    db_changes = msg_payload.get("change")
                    for change in db_changes:
                        # Publish changes to a RabbitMQ queue
                else:
                    print("\rListening for WAL changes...", end = "", flush = True)
            except KeyboardInterrupt:
                print("Closing replication connection...")
                cur.stop_replication()
        