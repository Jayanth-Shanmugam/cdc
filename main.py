# main.py
import argparse

from wal_consumer import WALConsumer
from rmq_producer import Producer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host", required=True)
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-password", required=True)

    parser.add_argument("--slot", required=True)

    parser.add_argument("--mq-host",  type=str, default='localhost')
    parser.add_argument("--mq-port",  type=int, default=5672)
    parser.add_argument("--mq-vhost", type=str, default='/')
    parser.add_argument("--mq-user",  type=str, default='guest')
    parser.add_argument("--mq-password", type=str, default='guest')
    parser.add_argument("--queue", required=True)

    args = parser.parse_args()

    reader = WALConsumer(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_db,
        username=args.pg_user,
        password=args.pg_password,
    )

    with Producer(
        host=args.mq_host,
        port=args.mq_port,
        vhost=args.mq_vhost,
        username=args.mq_user,
        password=args.mq_password,
        queue_name=args.queue,
    ) as producer:
        reader.consume(
            slot_name=args.slot,
            options={},
            decode=True,
            on_change=producer.publish,
        )


if __name__ == "__main__":
    main()