import os
import json
from dotenv import load_dotenv
import argparse
from kafka_wal_consumer import KafkaWALConsumer
from datetime import datetime
import duckdb

def process_wal(msg):
    payload = json.loads(msg.value.decode("utf-8"))

    changes = payload.get("change", [])
    received_at = datetime.utcnow()
    kafka_topic = msg.topic()

    # Connect to DuckDB
    conn = duckdb.connect("staging.db")

    # Create the staging table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS staging_inventory (
        warehouse_id INTEGER, 
        sku INTEGER, 
        product_description VARCHAR(30),
        build VARCHAR(20),
        quantity INTEGER, 
        rack_id INTEGER,
        palette_id INTEGER,
        updated_at TIMESTAMP,
        _change_type VARCHAR(10),
        _received_at TIMESTAMP,
        _kafka_topic VARCHAR,
        _processed BOOLEAN DEFAULT FALSE
    );
    """
    conn.execute(create_table_query)

    for change in changes:
        kind = change.get("kind", "")
        table = change.get("table", "")
        columnnames = change.get("columnnames", [])
        columnvalues = change.get("columnvalues", [])
        oldkeys = change.get("oldkeys", {})
        oldkeyvalues = oldkeys.get("keyvalues", [])

        match kind:
            case "insert":
                insert_query = f"""
                INSERT INTO staging_inventory (
                    {', '.join(columnnames)}, _change_type, _received_at, _kafka_topic, _processed
                ) VALUES ({', '.join(['?'] * len(columnvalues))}, ?, ?, ?, ?);
                """
                conn.execute(insert_query, (*columnvalues, kind, received_at, kafka_topic, False))

            case "update":
                insert_query = f"""
                INSERT INTO staging_inventory (
                    {', '.join(columnnames)}, _change_type, _received_at, _kafka_topic, _processed
                ) VALUES ({', '.join(['?'] * len(columnvalues))}, ?, ?, ?, ?);
                """
                conn.execute(insert_query, (*columnvalues, kind, received_at, kafka_topic, False))

            case "delete":
                # For delete, only insert the old keys and set other columns to NULL
                insert_query = """
                INSERT INTO staging_inventory (
                    warehouse_id, sku, product_description, build, quantity, rack_id, palette_id, updated_at, 
                    _change_type, _received_at, _kafka_topic, _processed
                ) VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?);
                """
                conn.execute(insert_query, (*oldkeyvalues, kind, received_at, kafka_topic, False))

    # Commit and close the connection
    conn.commit()
    conn.close()

def main():
    load_dotenv()

    cluster_config = {
        "bootstrap.servers": os.getenv("KAFKA_CLUSTER_BOOTSTRAP_SERVERS"),
        "sasl.username": os.getenv("KAFKA_CLUSTER_API_KEY"),
        "sasl.password": os.getenv("KAFKA_CLUSTER_API_SECRET"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-topic", type=str, required=True)
    args = parser.parse_args()

    consumer = KafkaWALConsumer(cluster_config=cluster_config, topic=args.kafka_topic)
    consumer.consume(callback=process_wal)

if __name__ == "__main__":
    main()