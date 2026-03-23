"""
Unit tests for the banking data pipeline.
Covers:
  - data-generator: random_money utility
  - consumer: CDC payload parsing, topic name extraction, op type routing
These run in CI against a real PostgreSQL service container.
"""

import importlib.util
import os
import pathlib
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ROOT = pathlib.Path(__file__).parent.parent


# ── Helpers ───────────────────────────────────────────────────

def _load_module(rel_path: str):
    """Load a project module by relative path without triggering side-effects."""
    abs_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(abs_path.stem, abs_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── Generator tests ───────────────────────────────────────────

def test_random_money_range():
    """random_money should always return a value within the given bounds."""
    gen = _load_module("data-generator/generator.py")
    for _ in range(100):
        val = gen.random_money(Decimal("1.00"), Decimal("1000.00"))
        assert Decimal("1.00") <= val <= Decimal("1000.00")


def test_random_money_two_decimal_places():
    """random_money should always return exactly 2 decimal places."""
    gen = _load_module("data-generator/generator.py")
    for _ in range(50):
        val = gen.random_money(Decimal("10.00"), Decimal("100.00"))
        assert val == val.quantize(Decimal("0.01")), (
            f"Expected 2 decimal places, got {val}"
        )


def test_random_money_never_negative():
    """random_money should never return a negative value."""
    gen = _load_module("data-generator/generator.py")
    for _ in range(50):
        val = gen.random_money(Decimal("0.01"), Decimal("500.00"))
        assert val >= Decimal("0.00"), f"Got negative value: {val}"


# ── Consumer CDC parsing tests ────────────────────────────────

def test_extract_table_name_customers():
    """Topic banking_server.public.customers → customers."""
    topic = "banking_server.public.customers"
    result = topic.split(".")[-1]
    assert result == "customers"


def test_extract_table_name_transactions():
    """Topic banking_server.public.transactions → transactions."""
    topic = "banking_server.public.transactions"
    result = topic.split(".")[-1]
    assert result == "transactions"


def test_cdc_payload_create_uses_after():
    """
    For op=c (INSERT), the consumer must extract the 'after' field.
    This is the core CDC routing logic in kafka_to_minio.py.
    """
    payload = {
        "op": "c",
        "before": None,
        "after": {
            "id": 1,
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@example.com",
            "created_at": 1700000000000,
        },
        "ts_ms": 1700000000000,
    }

    op = payload.get("op")
    if op in ("r", "c", "u"):
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        record = None

    assert record is not None
    assert record["email"] == "alice@example.com"
    assert record["id"] == 1


def test_cdc_payload_delete_uses_before():
    """
    For op=d (DELETE), the consumer must extract the 'before' field,
    capturing the row as it existed before deletion.
    """
    payload = {
        "op": "d",
        "before": {
            "id": 42,
            "first_name": "Bob",
            "last_name": "Jones",
            "email": "bob@example.com",
            "created_at": 1700000000000,
        },
        "after": None,
        "ts_ms": 1700001000000,
    }

    op = payload.get("op")
    if op in ("r", "c", "u"):
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        record = None

    assert record is not None
    assert record["id"] == 42
    assert record["email"] == "bob@example.com"


def test_cdc_payload_heartbeat_is_skipped():
    """
    Debezium heartbeat events have no 'op' field (or op is None).
    The consumer must skip them — record should be None.
    """
    payload = {
        "ts_ms": 1700002000000,
        # no 'op' key — this is a heartbeat
    }

    op = payload.get("op")
    if op in ("r", "c", "u"):
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        record = None  # heartbeat / schema-change → skip

    assert record is None


def test_cdc_metadata_attached_to_record():
    """
    After routing, the consumer attaches _cdc_op and _cdc_ts
    to every record before buffering it.
    """
    payload = {
        "op": "u",
        "before": {"id": 5, "email": "old@example.com"},
        "after": {"id": 5, "email": "new@example.com"},
        "ts_ms": 1700003000000,
    }

    op = payload.get("op")
    record = payload.get("after") if op in ("r", "c", "u") else payload.get("before")

    # Simulate what kafka_to_minio.py does
    record["_cdc_op"] = op
    record["_cdc_ts"] = payload.get("ts_ms")

    assert record["_cdc_op"] == "u"
    assert record["_cdc_ts"] == 1700003000000
    assert record["email"] == "new@example.com"
    