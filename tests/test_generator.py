"""
Unit tests for the banking data generator.
These run in CI against a real PostgreSQL service container.
"""

import importlib.util
import os
import sys
from decimal import Decimal

import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_random_money_range():
    """random_money should always return a value within bounds."""
    spec = importlib.util.spec_from_file_location(
        "generator",
        importlib.util.find_spec("pathlib") and
        __import__("pathlib").Path(__file__).parent.parent / "data-generator" / "generator.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for _ in range(100):
        val = module.random_money(Decimal("1.00"), Decimal("1000.00"))
        assert Decimal("1.00") <= val <= Decimal("1000.00")
        assert val == round(val, 2)


def test_random_money_two_decimal_places():
    """random_money should always return exactly 2 decimal places."""
    spec = importlib.util.spec_from_file_location(
        "generator",
        __import__("pathlib").Path(__file__).parent.parent / "data-generator" / "generator.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    val = module.random_money(Decimal("10.00"), Decimal("100.00"))
    assert val == val.quantize(Decimal("0.01"))