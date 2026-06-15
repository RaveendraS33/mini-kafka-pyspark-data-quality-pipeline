import argparse
import json
import time
from typing import Iterable

from kafka import KafkaProducer

try:
    from producer import add_bad_values, create_good_record
except ImportError:
    from producer.producer import add_bad_values, create_good_record


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "transactions"


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda record: json.dumps(record).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )


def generate_batch(
    start_record_id: int,
    records_per_batch: int,
    bad_records_per_batch: int,
) -> Iterable[dict]:
    for index in range(records_per_batch):
        record = create_good_record(start_record_id + index)

        if index < bad_records_per_batch:
            record = add_bad_values(record)

        yield record


def stream_records_to_kafka(
    topic: str,
    bootstrap_servers: str,
    total_batches: int,
    records_per_batch: int,
    bad_records_per_batch: int,
    sleep_seconds: float,
) -> None:
    producer = build_kafka_producer(bootstrap_servers)
    record_id = 1

    try:
        for batch_number in range(1, total_batches + 1):
            sent_count = 0

            for record in generate_batch(
                start_record_id=record_id,
                records_per_batch=records_per_batch,
                bad_records_per_batch=bad_records_per_batch,
            ):
                producer.send(
                    topic,
                    key=record["transaction_id"] or f"missing-key-{record_id}",
                    value=record,
                )
                record_id += 1
                sent_count += 1

            producer.flush()
            print(f"Batch {batch_number}: sent {sent_count} records to topic '{topic}'")
            time.sleep(sleep_seconds)
    finally:
        producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish synthetic transactions to Kafka.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--records-per-batch", type=int, default=10)
    parser.add_argument("--bad-records-per-batch", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stream_records_to_kafka(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        total_batches=args.batches,
        records_per_batch=args.records_per_batch,
        bad_records_per_batch=args.bad_records_per_batch,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
