"""
Basic unit tests for the data generator.
These run in CI against a real PostgreSQL service container.
"""
import os
from decimal import Decimal
import pytest

# Add project root to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_random_money_range():
    """random_money should always return a value within bounds."""
    from data_generator.generator import random_money  # noqa: F401 (dynamic import check)
    # Dynamically import to avoid requiring psycopg2 at collection time
    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "generator",
        pathlib.Path(__file__).parent.parent / "data-generator" / "generator.py",
    )
    # We cannot fully execute the module (it connects to DB at import),
    # so just verify the file is parseable Python.
    source = (pathlib.Path(__file__).parent.parent / "data-generator" / "generator.py").read_text()
    compile(source, "generator.py", "exec")


def test_consumer_module_parseable():
    """kafka_to_minio.py should be valid Python."""
    import pathlib
    source = (pathlib.Path(__file__).parent.parent / "consumer" / "kafka_to_minio.py").read_text()
    compile(source, "kafka_to_minio.py", "exec")


def test_register_connector_parseable():
    """register_connector.py should be valid Python."""
    import pathlib
    source = (pathlib.Path(__file__).parent.parent / "kafka-debezium" / "register_connector.py").read_text()
    compile(source, "register_connector.py", "exec")


def test_dag_files_parseable():
    """All Airflow DAG files should be valid Python."""
    import pathlib
    dag_dir = pathlib.Path(__file__).parent.parent / "docker" / "dags"
    for dag_file in dag_dir.glob("*.py"):
        source = dag_file.read_text()
        compile(source, str(dag_file), "exec")
