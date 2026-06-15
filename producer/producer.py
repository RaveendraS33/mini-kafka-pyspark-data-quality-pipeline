import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from faker import Faker

fake = Faker()

OUTPUT_PATH = Path("output/sample_stream_data.jsonl")

def create_good_record(record_id):
    return {
        "transaction_id": f"TXN{record_id:04d}",
        "user_id": random.randint(1000, 9999),
        "name": fake.name(),
        "email": fake.email(),
        "amount": round(random.uniform(5, 500), 2),
        "currency": "USD",
        "city": fake.city(),
        "device": random.choice(["mobile", "web", "tablet"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(["SUCCESS", "FAILED", "PENDING"]),
    }

def add_bad_values(record):
    bad_field = random.choice(["transaction_id", "user_id", "email", "amount", "event_time"])

    if bad_field in ["transaction_id", "user_id", "event_time"]:
        record[bad_field] = None
    elif bad_field == "email":
        record[bad_field] = "invalid_email"
    elif bad_field == "amount":
        record[bad_field] = -50

    return record


def main():
    total_records = 100
    records_per_second = 10
    bad_records_per_batch = 3

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        record_id = 1

        for second in range(10):
            batch = []

            for i in range(records_per_second):
                record = create_good_record(record_id)

                if i < bad_records_per_batch:
                    record = add_bad_values(record)

                batch.append(record)
                file.write(json.dumps(record) + "\n")
                record_id += 1

            print(f"Second {second + 1}: generated {len(batch)} records")
            time.sleep(1)

    print(f"\nDone. Generated {total_records} records.")
    print(f"Saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()