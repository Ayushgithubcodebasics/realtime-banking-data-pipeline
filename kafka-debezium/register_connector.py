"""Register or update the Debezium PostgreSQL connector."""

from __future__ import annotations

import json
import os
import sys
import time

import requests

from common.config import load_project_env

load_project_env()

CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083/connectors")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "banking-postgres-connector")

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_CONTAINER_HOST", "postgres"),
        "database.port": os.getenv("POSTGRES_CONTAINER_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER", "postgres"),
        "database.password": os.getenv("POSTGRES_PASSWORD", "postgres123"),
        "database.dbname": os.getenv("POSTGRES_DB", "banking"),
        "topic.prefix": os.getenv("DEBEZIUM_TOPIC_PREFIX", "banking_server"),
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "publication.name": "banking_publication",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "tombstones.on.delete": "false",
        "snapshot.mode": "initial",
        "heartbeat.interval.ms": "30000",
    },
}


def wait_for_connect(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    print(f"Waiting for Debezium Connect at {CONNECT_URL}")
    while time.time() < deadline:
        try:
            resp = requests.get(CONNECT_URL, timeout=5)
            if resp.status_code == 200:
                print("✅ Debezium Connect is reachable")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise RuntimeError("Timed out waiting for Debezium Connect")


def connector_url(suffix: str = "") -> str:
    base = f"{CONNECT_URL}/{CONNECTOR_NAME}"
    return f"{base}{suffix}"


def register_connector() -> None:
    headers = {"Content-Type": "application/json"}
    check = requests.get(connector_url(), timeout=10)

    if check.status_code == 404:
        resp = requests.post(
            CONNECT_URL,
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG),
            timeout=10,
        )
        if resp.status_code != 201:
            raise RuntimeError(f"Failed to create connector: {resp.status_code} {resp.text}")
        print(f"✅ Created connector '{CONNECTOR_NAME}'")
        return

    if check.status_code != 200:
        raise RuntimeError(f"Unexpected status {check.status_code}: {check.text}")

    print(f"Connector '{CONNECTOR_NAME}' already exists. Updating config.")
    resp = requests.put(
        connector_url("/config"),
        headers=headers,
        data=json.dumps(CONNECTOR_CONFIG["config"]),
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to update connector: {resp.status_code} {resp.text}")
    print(f"✅ Updated connector '{CONNECTOR_NAME}'")


def wait_for_running(timeout: int = 90) -> None:
    deadline = time.time() + timeout
    print("Waiting for connector tasks to enter RUNNING state")
    while time.time() < deadline:
        resp = requests.get(connector_url("/status"), timeout=5)
        if resp.status_code == 200:
            status = resp.json()
            connector_state = status.get("connector", {}).get("state", "")
            task_states = [task.get("state", "") for task in status.get("tasks", [])]
            print(f"  connector={connector_state} tasks={task_states}")
            if connector_state == "RUNNING" and task_states and all(
                state == "RUNNING" for state in task_states
            ):
                print("✅ CDC connector is RUNNING")
                return
            if connector_state == "FAILED" or "FAILED" in task_states:
                raise RuntimeError(json.dumps(status, indent=2))
        time.sleep(5)
    raise RuntimeError("Connector did not reach RUNNING state in time")


def main() -> None:
    try:
        wait_for_connect()
        register_connector()
        wait_for_running()
    except Exception as exc:  # noqa: BLE001
        print(f"❌ {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
