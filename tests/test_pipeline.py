"""Unit tests for the banking data pipeline."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
from decimal import Decimal

ROOT = pathlib.Path(__file__).parent.parent


def load_module(rel_path: str):
    abs_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(abs_path.stem, abs_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[abs_path.stem] = module
    spec.loader.exec_module(module)
    return module


def test_random_money_range() -> None:
    gen = load_module("data-generator/generator.py")
    for _ in range(100):
        value = gen.random_money(Decimal("1.00"), Decimal("1000.00"))
        assert Decimal("1.00") <= value <= Decimal("1000.00")


def test_random_money_two_decimal_places() -> None:
    gen = load_module("data-generator/generator.py")
    value = gen.random_money(Decimal("10.00"), Decimal("100.00"))
    assert value == value.quantize(Decimal("0.01"))


def test_build_arg_parser_supports_once_flag() -> None:
    gen = load_module("data-generator/generator.py")
    args = gen.build_arg_parser().parse_args(["--once"])
    assert args.once is True


def test_extract_table_name() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    assert consumer.extract_table_name("banking_server.public.transactions") == "transactions"


def test_route_cdc_record_create_uses_after() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    record, op, ts = consumer.route_cdc_record(
        {
            "payload": {
                "op": "c",
                "after": {"id": 1, "email": "alice@example.com"},
                "before": None,
                "ts_ms": 1700000000000,
            }
        }
    )
    assert op == "c"
    assert ts == 1700000000000
    assert record["email"] == "alice@example.com"
    assert record["_is_deleted"] is False


def test_route_cdc_record_delete_uses_before() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    record, op, _ = consumer.route_cdc_record(
        {
            "payload": {
                "op": "d",
                "before": {"id": 42, "email": "bob@example.com"},
                "after": None,
                "ts_ms": 1700001000000,
            }
        }
    )
    assert op == "d"
    assert record["id"] == 42
    assert record["_is_deleted"] is True


def test_route_cdc_record_skips_heartbeat() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    record, op, ts = consumer.route_cdc_record({"payload": {"ts_ms": 1700002000000}})
    assert record is None
    assert op is None
    assert ts is None


def test_normalize_record_casts_money_to_string() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    normalized = consumer.normalize_record(
        "accounts",
        {"id": 1, "balance": Decimal("123.45"), "_cdc_op": "u", "_cdc_ts": 1},
    )
    assert normalized["balance"] == "123.45"


def test_should_flush_for_batch_size() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    assert consumer.should_flush(last_flush_at=0.0, record_count=consumer.BATCH_SIZE) is True


def test_should_flush_for_elapsed_time() -> None:
    consumer = load_module("consumer/kafka_to_minio.py")
    assert consumer.should_flush(
        last_flush_at=0.0,
        record_count=1,
    ) is True
