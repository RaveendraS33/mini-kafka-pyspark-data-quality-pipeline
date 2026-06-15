import json
from pathlib import Path
from datetime import datetime, timezone


INPUT_PATH = Path("output/sample_stream_data.jsonl")
CLEAN_OUTPUT_PATH = Path("output/clean_data/clean_records.jsonl")
BAD_OUTPUT_PATH = Path("output/bad_records/bad_records.jsonl")
HEALTH_REPORT_PATH = Path("output/health_report/pipeline_health_report.json")


def is_valid_record(record):
    if record.get("transaction_id") is None:
        return False, "missing_transaction_id"

    if record.get("user_id") is None:
        return False, "missing_user_id"

    if record.get("email") is None or "@" not in record.get("email"):
        return False, "invalid_email"

    if record.get("amount") is None or record.get("amount") <= 0:
        return False, "invalid_amount"

    if record.get("event_time") is None:
        return False, "missing_event_time"

    return True, "valid"


def main():
    total_records = 0
    clean_records = 0
    bad_records = 0

    error_counts = {}

    CLEAN_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    BAD_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    HEALTH_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with INPUT_PATH.open("r", encoding="utf-8") as input_file, \
            CLEAN_OUTPUT_PATH.open("w", encoding="utf-8") as clean_file, \
            BAD_OUTPUT_PATH.open("w", encoding="utf-8") as bad_file:

        for line in input_file:
            total_records += 1
            record = json.loads(line)

            is_valid, reason = is_valid_record(record)

            if is_valid:
                clean_records += 1
                clean_file.write(json.dumps(record) + "\n")
            else:
                bad_records += 1
                record["error_reason"] = reason
                bad_file.write(json.dumps(record) + "\n")

                error_counts[reason] = error_counts.get(reason, 0) + 1

    health_report = {
        "pipeline_name": "mini-kafka-pyspark-data-quality-pipeline",
        "run_time": datetime.now(timezone.utc).isoformat(),
        "input_file": str(INPUT_PATH),
        "total_records": total_records,
        "clean_records": clean_records,
        "bad_records": bad_records,
        "data_quality_score": round((clean_records / total_records) * 100, 2) if total_records > 0 else 0,
        "error_counts": error_counts,
        "status": "SUCCESS"
    }

    with HEALTH_REPORT_PATH.open("w", encoding="utf-8") as report_file:
        json.dump(health_report, report_file, indent=4)

    print("Data quality job completed.")
    print(f"Total records: {total_records}")
    print(f"Clean records: {clean_records}")
    print(f"Bad records: {bad_records}")
    print(f"Health report saved to: {HEALTH_REPORT_PATH}")


if __name__ == "__main__":
    main()