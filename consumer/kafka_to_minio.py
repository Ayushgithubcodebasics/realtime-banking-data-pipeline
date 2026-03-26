"""Kafka -> MinIO CDC consumer.

Consumes Debezium events and writes table-specific Parquet batches to MinIO.
Improvements over the original version:
  - no side effects at import time
  - manual Kafka commits after successful upload
  - DLQ support for malformed events
  - flush by batch size or time window
  - consistent metadata columns for downstream modeling
"""

from __future__ import annotations

import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

try:
    from common.config import env_int, load_project_env
except ModuleNotFoundError:  # pragma: no cover - direct script execution fallback
    import sys
    from pathlib import Path

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from common.config import env_int, load_project_env

load_project_env()

TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]
BATCH_SIZE = env_int("CONSUMER_BATCH_SIZE", 50)
FLUSH_INTERVAL_SECONDS = env_int("CONSUMER_FLUSH_INTERVAL_SECONDS", 20)
UPLOAD_RETRIES = env_int("CONSUMER_UPLOAD_RETRIES", 3)
BUCKET = os.getenv("MINIO_BUCKET", "raw")
DLQ_PREFIX = os.getenv("MINIO_DLQ_PREFIX", "dlq")


def extract_table_name(topic: str) -> str:
    return topic.split(".")[-1]


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092").split(","),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=False,
        group_id=os.getenv("KAFKA_GROUP", "minio-consumer-group"),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_records=500,
    )


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    )


def ensure_bucket_exists(s3_client, bucket: str) -> None:
    existing = {bucket_info["Name"] for bucket_info in s3_client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)
        print(f"✅ Created MinIO bucket: {bucket}")
    else:
        print(f"✅ MinIO bucket exists: {bucket}")


def route_cdc_record(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, int | None]:
    payload = event.get("payload", event)
    op = payload.get("op")

    if op in {"r", "c", "u"}:
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        return None, None, None

    if not isinstance(record, dict):
        return None, None, None

    enriched = dict(record)
    enriched["_cdc_op"] = op
    enriched["_cdc_ts"] = payload.get("ts_ms")
    enriched["_is_deleted"] = op == "d"
    return enriched, op, payload.get("ts_ms")


def normalize_record(table_name: str, record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    if table_name in {"accounts", "transactions"}:
        for field in ("balance", "amount"):
            if field in normalized and normalized[field] is not None:
                normalized[field] = str(normalized[field])
    return normalized


def parquet_key(table_name: str) -> str:
    now = datetime.now(tz=timezone.utc)
    return (
        f"{table_name}/date={now.strftime('%Y-%m-%d')}/"
        f"{table_name}_{now.strftime('%H%M%S_%f')}.parquet"
    )


def upload_bytes(s3_client, bucket: str, key: str, payload: bytes) -> None:
    last_error: Exception | None = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            s3_client.upload_fileobj(io.BytesIO(payload), bucket, key)
            return
        except (BotoCoreError, ClientError) as exc:
            last_error = exc
            time.sleep(attempt)
    raise RuntimeError(f"Failed to upload {key}: {last_error}")


def write_to_minio(s3_client, table_name: str, records: list[dict[str, Any]]) -> str:
    if not records:
        raise ValueError("records must not be empty")

    df = pd.DataFrame(records)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf)
    key = parquet_key(table_name)
    upload_bytes(s3_client, BUCKET, key, buf.getvalue())
    print(f"  📦 Uploaded {len(records):>4} records -> s3://{BUCKET}/{key}")
    return key


def dlq_key() -> str:
    now = datetime.now(tz=timezone.utc)
    return f"{DLQ_PREFIX}/date={now.strftime('%Y-%m-%d')}/bad_events_{now.strftime('%H%M%S_%f')}.jsonl"


def write_dlq_event(s3_client, raw_event: Any, reason: str) -> None:
    payload = json.dumps({"reason": reason, "event": raw_event}, default=str).encode("utf-8") + b"\n"
    upload_bytes(s3_client, BUCKET, dlq_key(), payload)
    print(f"  ⚠️ Sent malformed event to DLQ ({reason})")


def commit_offsets(consumer: KafkaConsumer, offset_map: dict[tuple[str, int], int]) -> None:
    if not offset_map:
        return
    commit_payload = {
        TopicPartition(topic, partition): OffsetAndMetadata(offset, None)
        for (topic, partition), offset in offset_map.items()
    }
    consumer.commit(offsets=commit_payload)


def should_flush(last_flush_at: float, record_count: int) -> bool:
    return record_count >= BATCH_SIZE or (time.time() - last_flush_at) >= FLUSH_INTERVAL_SECONDS


def flush_topic(
    consumer: KafkaConsumer,
    s3_client,
    topic: str,
    buffers: dict[str, list[dict[str, Any]]],
    offset_buffers: dict[str, dict[tuple[str, int], int]],
) -> None:
    records = buffers[topic]
    if not records:
        return
    table_name = extract_table_name(topic)
    write_to_minio(s3_client, table_name, records)
    commit_offsets(consumer, offset_buffers[topic])
    buffers[topic] = []
    offset_buffers[topic] = {}


def process_messages() -> None:
    consumer = create_consumer()
    s3_client = create_s3_client()
    ensure_bucket_exists(s3_client, BUCKET)

    buffers: dict[str, list[dict[str, Any]]] = {topic: [] for topic in TOPICS}
    offset_buffers: dict[str, dict[tuple[str, int], int]] = {topic: {} for topic in TOPICS}
    last_flush_at: dict[str, float] = defaultdict(time.time)

    print("🚀 Consumer started")
    print(f"   Topics : {TOPICS}")
    print(f"   MinIO  : {os.getenv('MINIO_ENDPOINT')} bucket={BUCKET}")

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in polled.items():
                for message in messages:
                    event = message.value
                    if not isinstance(event, dict):
                        write_dlq_event(s3_client, event, "non-dict event")
                        continue

                    record, op, _ = route_cdc_record(event)
                    if record is None or op is None:
                        continue

                    topic = message.topic
                    table_name = extract_table_name(topic)
                    normalized = normalize_record(table_name, record)
                    buffers[topic].append(normalized)
                    offset_buffers[topic][(topic, topic_partition.partition)] = message.offset + 1
                    print(f"  [{table_name:12s}] op={op} id={normalized.get('id', '?')}")

                    if should_flush(last_flush_at[topic], len(buffers[topic])):
                        flush_topic(consumer, s3_client, topic, buffers, offset_buffers)
                        last_flush_at[topic] = time.time()

            for topic in TOPICS:
                if buffers[topic] and should_flush(last_flush_at[topic], len(buffers[topic])):
                    flush_topic(consumer, s3_client, topic, buffers, offset_buffers)
                    last_flush_at[topic] = time.time()
    except KeyboardInterrupt:
        print("\nInterrupted. Flushing buffered records.")
    finally:
        for topic in TOPICS:
            if buffers[topic]:
                flush_topic(consumer, s3_client, topic, buffers, offset_buffers)
        consumer.close()
        print("✅ Consumer shut down cleanly")


if __name__ == "__main__":
    process_messages()
