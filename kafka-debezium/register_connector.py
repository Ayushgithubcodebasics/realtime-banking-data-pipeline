"""
Debezium Connector Registration Script
Registers (or updates) the PostgreSQL CDC connector with Debezium Connect.

Run this AFTER docker-compose services are healthy:
    python register_connector.py

What it does:
  - POSTs the connector config to Debezium Connect REST API
  - If connector already exists, it PUTs an update instead
  - Polls the connector status until it shows RUNNING
"""

import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Config ────────────────────────────────────────────────────
DEBEZIUM_URL = (
    f"http://{os.getenv('DEBEZIUM_HOST', 'localhost')}"
    f":{os.getenv('DEBEZIUM_PORT', '8083')}"
    f"/connectors"
)
CONNECTOR_NAME = "banking-postgres-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # ── PostgreSQL connection ────────────────────────────
        "database.hostname": "postgres",
        "database.port": os.getenv("POSTGRES_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER", "postgres"),
        "database.password": os.getenv("POSTGRES_PASSWORD", "postgres123"),
        "database.dbname": os.getenv("POSTGRES_DB", "banking"),
        # ── Topic prefix (becomes: banking_server.public.customers, etc.) ──
        "topic.prefix": "banking_server",
        # ── Tables to capture (schema.table format) ─────────
        "table.include.list": "public.customers,public.accounts,public.transactions",
        # ── Replication plugin (pgoutput is built into PG 10+) ──
        "plugin.name": "pgoutput",
        # ── Replication slot name (must match init.sql) ─────
        "slot.name": "banking_slot",
        # ── Publication mode: only capture the listed tables ─
        "publication.autocreate.mode": "filtered",
        "publication.name": "banking_publication",
        # ── Data type handling ───────────────────────────────
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect",
        # ── Tombstone events for deletes ─────────────────────
        "tombstones.on.delete": "false",
        # ── Snapshot mode: read existing rows first ──────────
        "snapshot.mode": "initial",
        # ── Heartbeat to keep replication slot active ────────
        "heartbeat.interval.ms": "30000",
    },
}


def wait_for_connect(timeout: int = 120) -> None:
    """Wait until Debezium Connect REST API is reachable."""
    print(f"⏳ Waiting for Debezium Connect at {DEBEZIUM_URL} …")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(DEBEZIUM_URL, timeout=5)
            if resp.status_code == 200:
                print("✅ Debezium Connect is ready")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    print("❌ Timed out waiting for Debezium Connect")
    sys.exit(1)


def register_connector() -> None:
    """POST or PUT the connector config."""
    headers = {"Content-Type": "application/json"}

    # Check if connector already exists
    check = requests.get(f"{DEBEZIUM_URL}/{CONNECTOR_NAME}", timeout=10)

    if check.status_code == 404:
        # Create new
        resp = requests.post(
            DEBEZIUM_URL,
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG),
            timeout=10,
        )
        if resp.status_code == 201:
            print(f"✅ Connector '{CONNECTOR_NAME}' created successfully")
        else:
            print(f"❌ Failed to create connector ({resp.status_code}): {resp.text}")
            sys.exit(1)

    elif check.status_code == 200:
        # Update existing connector config
        print(f"⚠️  Connector '{CONNECTOR_NAME}' already exists — updating config …")
        put_url = f"{DEBEZIUM_URL}/{CONNECTOR_NAME}/config"
        resp = requests.put(
            put_url,
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG["config"]),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            print(f"✅ Connector '{CONNECTOR_NAME}' updated successfully")
        else:
            print(f"❌ Failed to update connector ({resp.status_code}): {resp.text}")
            sys.exit(1)
    else:
        print(f"❌ Unexpected status {check.status_code}: {check.text}")
        sys.exit(1)


def wait_for_running(timeout: int = 60) -> None:
    """Poll connector status until it shows RUNNING."""
    print("⏳ Waiting for connector to reach RUNNING state …")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(
                f"{DEBEZIUM_URL}/{CONNECTOR_NAME}/status", timeout=5
            )
            if resp.status_code == 200:
                status = resp.json()
                connector_state = status.get("connector", {}).get("state", "")
                tasks = status.get("tasks", [])
                task_states = [t.get("state", "") for t in tasks]
                print(f"   Connector: {connector_state} | Tasks: {task_states}")
                if connector_state == "RUNNING" and all(
                    s == "RUNNING" for s in task_states
                ):
                    print("✅ Connector is RUNNING — CDC is active!")
                    return
                if connector_state == "FAILED" or "FAILED" in task_states:
                    print(f"❌ Connector FAILED: {json.dumps(status, indent=2)}")
                    sys.exit(1)
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    print("⚠️  Connector did not reach RUNNING state in time — check Debezium logs")


if __name__ == "__main__":
    wait_for_connect()
    register_connector()
    wait_for_running()
