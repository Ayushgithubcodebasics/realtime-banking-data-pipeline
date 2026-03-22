"""
Kafka → MinIO Consumer
Consumes CDC events from the three banking Kafka topics and writes them
to MinIO as Parquet files, partitioned by date.

Key fixes over the original:
  - Uses kafka-python-ng (maintained fork; kafka-python is abandoned)
  - Uses pyarrow instead of broken fastparquet for Parquet serialisation
  - Handles all CDC operation types: r (read/snapshot), c (create),
    u (update), d (delete)
  - Writes to a temp file via BytesIO → avoids disk clutter
  - Correctly handles Debezium's nested payload → after structure
  - Uses pandas nullable dtypes so NaN integers (related_account_id)
    don't crash pyarrow

Usage:
    pip install -r requirements.txt
    python kafka_to_minio.py
"""

import io
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(Path(__file__).parent / ".env")

# ── Kafka topics ──────────────────────────────────────────────
TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

# ── Kafka consumer ────────────────────────────────────────────
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092").split(","),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP", "minio-consumer-group"),
    value_deserializer=lambda x: __import__("json").loads(x.decode("utf-8")),
    consumer_timeout_ms=-1,   # block forever (use Ctrl-C to stop)
    session_timeout_ms=30_000,
    heartbeat_interval_ms=10_000,
    max_poll_records=500,
)

# ── MinIO / S3 client ─────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
)

BUCKET = os.getenv("MINIO_BUCKET", "raw")

# Create bucket if it doesn't exist
existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)
    print(f"✅ Created MinIO bucket: {BUCKET}")
else:
    print(f"✅ MinIO bucket exists: {BUCKET}")

# ── In-memory buffer (topic → list of records) ────────────────
BATCH_SIZE = 50   # flush to MinIO every N records per topic
buffer: dict[str, list[dict]] = {t: [] for t in TOPICS}


def extract_table_name(topic: str) -> str:
    """banking_server.public.customers → customers"""
    return topic.split(".")[-1]


def write_to_minio(table_name: str, records: list[dict]) -> None:
    """Convert records to Parquet and upload to MinIO."""
    if not records:
        return

    df = pd.DataFrame(records)

    # Use pandas nullable integer types so NaN integers survive Parquet round-trip
    for col in df.select_dtypes(include=["float64"]).columns:
        # Only convert columns that look like IDs (whole numbers only)
        if df[col].dropna().apply(lambda x: x == int(x)).all():
            df[col] = pd.array(
                df[col].where(df[col].notna(), None), dtype=pd.Int64Dtype()
            )

    # Convert to Parquet in memory (no temp files on disk)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf)
    buf.seek(0)

    # S3 key: table/date=YYYY-MM-DD/HHMMSS_ffffff.parquet
    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%H%M%S_%f")
    s3_key = f"{table_name}/date={date_str}/{table_name}_{ts_str}.parquet"

    s3.upload_fileobj(buf, BUCKET, s3_key)
    print(
        f"  📦 Uploaded {len(records):>4} records → "
        f"s3://{BUCKET}/{s3_key}"
    )


# ── Main consume loop ─────────────────────────────────────────
print("🚀 Consumer started — listening for Kafka messages …")
print(f"   Topics : {TOPICS}")
print(f"   MinIO  : {os.getenv('MINIO_ENDPOINT')} / bucket={BUCKET}")
print("   Press Ctrl-C to stop\n")

try:
    for message in consumer:
        topic = message.topic
        event = message.value

        if not isinstance(event, dict):
            continue

        payload = event.get("payload", event)  # handle both wrapped and flat

        # Debezium operation types:
        #   r = read (snapshot), c = create, u = update, d = delete
        op = payload.get("op")
        if op in ("r", "c", "u"):
            record = payload.get("after")
        elif op == "d":
            record = payload.get("before")  # capture deleted row
        else:
            # Heartbeat or schema-change event — skip
            continue

        if record is None:
            continue

        # Add metadata
        record["_cdc_op"] = op
        record["_cdc_ts"] = payload.get("ts_ms")

        table = extract_table_name(topic)
        buffer[topic].append(record)
        print(f"  [{table:12s}] op={op} id={record.get('id', '?')}")

        if len(buffer[topic]) >= BATCH_SIZE:
            write_to_minio(table, buffer[topic])
            buffer[topic] = []

except KeyboardInterrupt:
    print("\n\n⚠️  Interrupted — flushing remaining buffers …")

finally:
    # Flush any remaining records
    for topic, records in buffer.items():
        if records:
            write_to_minio(extract_table_name(topic), records)
    consumer.close()
    print("✅ Consumer shut down cleanly")
