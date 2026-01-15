import json

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

class WALConsumer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "",
        username: str = "",
        password: str = "",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def consume(self, slot_name: str, options: dict, decode: bool, on_change) -> None:
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

        with psycopg2.connect(
            dbname = self.database,
            user = self.username,
            password = self.password,
            host = self.host,
            port = self.port,
            connection_factory = LogicalReplicationConnection,
        ) as conn:

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
                        for change in msg_payload.get("change", []):
                            on_change(change)
                        else:
                            cur.send_feedback(flush_lsn = msg.wal_end)
                    else:
                        print("\rListening for WAL changes...", end = "", flush = True)
                except KeyboardInterrupt:
                    print("Closing replication connection...")
                    break