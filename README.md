# Mini Kafka PySpark Data Quality Pipeline

A compact streaming data engineering project that generates synthetic transaction data, publishes it to Kafka, validates it with PySpark Structured Streaming, and separates clean records from bad records.

## Architecture

```text
producer.py
  -> producer_kafka.py
  -> Kafka topic: transactions
  -> PySpark streaming_cleaning_job.py
  -> output/clean_data
  -> output/bad_records
  -> output/health_report
```

## Features

- Synthetic transaction generation with Faker
- Intentional data quality issues for testing
- Kafka producer for local streaming
- PySpark Structured Streaming Kafka consumer
- Validation rules for missing IDs, invalid emails, negative amounts, and missing event times
- Separate clean and bad record outputs
- Streaming health summary grouped by validation result

## Project Structure

```text
mini-kafka-pyspark-data-quality-pipeline/
├── producer/
│   ├── producer.py
│   └── producer_kafka.py
├── spark_jobs/
│   └── streaming_cleaning_job.py
├── output/
│   ├── clean_data/
│   ├── bad_records/
│   └── health_report/
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

## Setup

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start Kafka:

```powershell
docker compose up -d
docker ps
```

## Run the Pipeline

Start the PySpark streaming job in one terminal:

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 `
  spark_jobs/streaming_cleaning_job.py
```

In a second terminal, publish synthetic records to Kafka:

```powershell
python producer/producer_kafka.py
```

Optional producer settings:

```powershell
python producer/producer_kafka.py `
  --topic transactions `
  --bootstrap-servers localhost:9092 `
  --batches 10 `
  --records-per-batch 10 `
  --bad-records-per-batch 3 `
  --sleep-seconds 1
```

## Output

Spark writes streaming JSON output under:

- `output/clean_data/`
- `output/bad_records/`
- `output/health_report/`
- `output/checkpoints/`

The `output/` folder is ignored by Git because it contains generated runtime data.

## Data Quality Rules

A record is marked bad when:

- `transaction_id` is missing
- `user_id` is missing
- `email` is missing or does not contain `@`
- `amount` is missing or less than or equal to zero
- `event_time` is missing

## Notes

`producer/producer.py` is intentionally kept as the original file-based generator. `producer/producer_kafka.py` reuses its data generation functions and adds Kafka publishing without replacing the original portfolio-friendly example.
