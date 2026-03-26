"""Banking data generator.

Generates realistic banking-style activity in PostgreSQL so Debezium can emit
CDC events for inserts, updates, and deletes.

Usage:
    python data-generator/generator.py
    python data-generator/generator.py --once
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

import psycopg2
from faker import Faker
from psycopg2.extensions import connection

try:
    from common.config import env_float, env_int, load_project_env
except ModuleNotFoundError:  # pragma: no cover - direct script execution fallback
    from pathlib import Path

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from common.config import env_float, env_int, load_project_env

load_project_env()

fake = Faker()

CURRENCY = os.getenv("BANKING_CURRENCY", "USD")
SLEEP_SECONDS = env_int("GENERATOR_SLEEP_SECONDS", 5)
MAX_CUSTOMER_INSERTS = env_int("GENERATOR_CUSTOMER_INSERTS", 3)
MAX_ACCOUNT_INSERTS = env_int("GENERATOR_ACCOUNT_INSERTS", 3)
MAX_TRANSACTION_EVENTS = env_int("GENERATOR_TRANSACTION_EVENTS", 12)
MAX_CUSTOMER_UPDATES = env_int("GENERATOR_CUSTOMER_UPDATES", 2)
MAX_ACCOUNT_UPDATES = env_int("GENERATOR_ACCOUNT_UPDATES", 2)
MAX_DELETE_EVENTS = env_int("GENERATOR_DELETE_EVENTS", 1)
INITIAL_BALANCE_MIN = Decimal(str(env_float("INITIAL_BALANCE_MIN", 100.00)))
INITIAL_BALANCE_MAX = Decimal(str(env_float("INITIAL_BALANCE_MAX", 10000.00)))
MAX_TXN_AMOUNT = Decimal(str(env_float("MAX_TXN_AMOUNT", 1000.00)))
MINIMUM_CUSTOMERS = env_int("MINIMUM_CUSTOMERS", 10)
MINIMUM_ACCOUNTS = env_int("MINIMUM_ACCOUNTS", 15)


@dataclass(frozen=True)
class AccountRow:
    account_id: int
    customer_id: int
    balance: Decimal
    account_type: str


@dataclass(frozen=True)
class IterationStats:
    customers_inserted: int = 0
    customers_updated: int = 0
    customers_deleted: int = 0
    accounts_inserted: int = 0
    accounts_updated: int = 0
    accounts_deleted: int = 0
    deposits: int = 0
    withdrawals: int = 0
    transfers: int = 0

    def merge(self, **kwargs: int) -> "IterationStats":
        data = self.__dict__.copy()
        data.update({key: data.get(key, 0) + value for key, value in kwargs.items()})
        return IterationStats(**data)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Banking data generator")
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
    return parser


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """Return a Decimal with exactly 2 decimal places."""
    min_cents = int((min_val * 100).to_integral_value())
    max_cents = int((max_val * 100).to_integral_value())
    cents = random.randint(min_cents, max_cents)
    return (Decimal(cents) / Decimal("100")).quantize(Decimal("0.01"))


def get_connection() -> connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "banking"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def fetch_customer_ids(cur) -> list[int]:
    cur.execute("SELECT id FROM customers ORDER BY id")
    return [row[0] for row in cur.fetchall()]


def fetch_accounts(cur) -> list[AccountRow]:
    cur.execute(
        """
        SELECT id, customer_id, balance, account_type
        FROM accounts
        ORDER BY id
        """
    )
    return [
        AccountRow(
            account_id=row[0],
            customer_id=row[1],
            balance=Decimal(str(row[2])).quantize(Decimal("0.01")),
            account_type=row[3],
        )
        for row in cur.fetchall()
    ]


def insert_customer(cur) -> int:
    email = fake.unique.email()
    cur.execute(
        """
        INSERT INTO customers (first_name, last_name, email)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (fake.first_name(), fake.last_name(), email),
    )
    return cur.fetchone()[0]


def insert_account(cur, customer_id: int) -> int:
    cur.execute(
        """
        INSERT INTO accounts (customer_id, account_type, balance, currency)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (
            customer_id,
            random.choice(["SAVINGS", "CHECKING"]),
            random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
            CURRENCY,
        ),
    )
    return cur.fetchone()[0]


def update_random_customer(cur, customer_ids: Iterable[int]) -> bool:
    ids = list(customer_ids)
    if not ids:
        return False
    customer_id = random.choice(ids)
    cur.execute(
        """
        UPDATE customers
        SET email = %s
        WHERE id = %s
        """,
        (fake.unique.email(), customer_id),
    )
    return cur.rowcount == 1


def update_random_account_type(cur, accounts: list[AccountRow]) -> bool:
    if not accounts:
        return False
    account = random.choice(accounts)
    new_type = "CHECKING" if account.account_type == "SAVINGS" else "SAVINGS"
    cur.execute(
        """
        UPDATE accounts
        SET account_type = %s
        WHERE id = %s
        """,
        (new_type, account.account_id),
    )
    return cur.rowcount == 1


def apply_deposit(cur, account_id: int, amount: Decimal) -> None:
    cur.execute(
        "UPDATE accounts SET balance = balance + %s WHERE id = %s",
        (amount, account_id),
    )
    cur.execute(
        """
        INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status)
        VALUES (%s, 'DEPOSIT', %s, NULL, 'COMPLETED')
        """,
        (account_id, amount),
    )


def apply_withdrawal(cur, account_id: int, amount: Decimal) -> bool:
    cur.execute(
        """
        UPDATE accounts
        SET balance = balance - %s
        WHERE id = %s AND balance >= %s
        """,
        (amount, account_id, amount),
    )
    if cur.rowcount != 1:
        return False
    cur.execute(
        """
        INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status)
        VALUES (%s, 'WITHDRAWAL', %s, NULL, 'COMPLETED')
        """,
        (account_id, amount),
    )
    return True


def apply_transfer(cur, source_id: int, target_id: int, amount: Decimal) -> bool:
    cur.execute(
        """
        UPDATE accounts
        SET balance = balance - %s
        WHERE id = %s AND balance >= %s
        """,
        (amount, source_id, amount),
    )
    if cur.rowcount != 1:
        return False

    cur.execute(
        "UPDATE accounts SET balance = balance + %s WHERE id = %s",
        (amount, target_id),
    )
    cur.execute(
        """
        INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status)
        VALUES (%s, 'TRANSFER', %s, %s, 'COMPLETED')
        """,
        (source_id, amount, target_id),
    )
    return True


def create_transactions(cur, accounts: list[AccountRow], txn_events: int) -> IterationStats:
    if not accounts:
        return IterationStats()

    stats = IterationStats()
    for _ in range(txn_events):
        txn_type = random.choice(["DEPOSIT", "WITHDRAWAL", "TRANSFER"])
        amount = random_money(Decimal("1.00"), MAX_TXN_AMOUNT)

        if txn_type == "DEPOSIT":
            apply_deposit(cur, random.choice(accounts).account_id, amount)
            stats = stats.merge(deposits=1)
            continue

        if txn_type == "WITHDRAWAL":
            eligible = [a for a in accounts if a.balance >= amount]
            if not eligible:
                continue
            success = apply_withdrawal(cur, random.choice(eligible).account_id, amount)
            if success:
                stats = stats.merge(withdrawals=1)
            continue

        if len(accounts) < 2:
            continue
        source = random.choice(accounts)
        possible_targets = [a for a in accounts if a.account_id != source.account_id]
        if not possible_targets or source.balance < amount:
            continue
        target = random.choice(possible_targets)
        success = apply_transfer(cur, source.account_id, target.account_id, amount)
        if success:
            stats = stats.merge(transfers=1)

    return stats


def delete_unused_account(cur) -> bool:
    cur.execute(
        """
        WITH candidate AS (
            SELECT a.id
            FROM accounts a
            LEFT JOIN transactions t
              ON t.account_id = a.id OR t.related_account_id = a.id
            WHERE t.id IS NULL
            ORDER BY a.id DESC
            LIMIT 1
        )
        DELETE FROM accounts
        WHERE id IN (SELECT id FROM candidate)
        """
    )
    return cur.rowcount == 1


def delete_orphan_customer(cur) -> bool:
    cur.execute(
        """
        WITH candidate AS (
            SELECT c.id
            FROM customers c
            LEFT JOIN accounts a ON a.customer_id = c.id
            WHERE a.id IS NULL
            ORDER BY c.id DESC
            LIMIT 1
        )
        DELETE FROM customers
        WHERE id IN (SELECT id FROM candidate)
        """
    )
    return cur.rowcount == 1


def ensure_minimum_seed_data(cur) -> IterationStats:
    stats = IterationStats()
    customer_ids = fetch_customer_ids(cur)
    accounts = fetch_accounts(cur)

    while len(customer_ids) < MINIMUM_CUSTOMERS:
        customer_ids.append(insert_customer(cur))
        stats = stats.merge(customers_inserted=1)

    while len(accounts) < MINIMUM_ACCOUNTS:
        customer_id = random.choice(customer_ids)
        insert_account(cur, customer_id)
        accounts = fetch_accounts(cur)
        stats = stats.merge(accounts_inserted=1)

    return stats


def run_iteration(conn: connection) -> IterationStats:
    with conn:
        with conn.cursor() as cur:
            stats = ensure_minimum_seed_data(cur)

            customer_ids = fetch_customer_ids(cur)
            accounts = fetch_accounts(cur)

            for _ in range(random.randint(1, MAX_CUSTOMER_INSERTS)):
                customer_ids.append(insert_customer(cur))
                stats = stats.merge(customers_inserted=1)

            for _ in range(random.randint(1, MAX_ACCOUNT_INSERTS)):
                customer_id = random.choice(customer_ids)
                insert_account(cur, customer_id)
                stats = stats.merge(accounts_inserted=1)

            for _ in range(random.randint(0, MAX_CUSTOMER_UPDATES)):
                if update_random_customer(cur, customer_ids):
                    stats = stats.merge(customers_updated=1)

            accounts = fetch_accounts(cur)
            for _ in range(random.randint(0, MAX_ACCOUNT_UPDATES)):
                if update_random_account_type(cur, accounts):
                    stats = stats.merge(accounts_updated=1)
                accounts = fetch_accounts(cur)

            txn_stats = create_transactions(cur, fetch_accounts(cur), random.randint(5, MAX_TRANSACTION_EVENTS))
            stats = stats.merge(**txn_stats.__dict__)

            for _ in range(random.randint(0, MAX_DELETE_EVENTS)):
                if delete_unused_account(cur):
                    stats = stats.merge(accounts_deleted=1)
                elif delete_orphan_customer(cur):
                    stats = stats.merge(customers_deleted=1)

    return stats


def format_stats(stats: IterationStats) -> str:
    return (
        "customers +{customers_inserted}/{customers_updated}u/{customers_deleted}d | "
        "accounts +{accounts_inserted}/{accounts_updated}u/{accounts_deleted}d | "
        "txns dep={deposits} wd={withdrawals} trf={transfers}"
    ).format(**stats.__dict__)


def main() -> None:
    args = build_arg_parser().parse_args()

    print("🏦 Banking data generator starting")
    print(
        f"   Postgres: {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')} "
        f"db={os.getenv('POSTGRES_DB', 'banking')}"
    )

    conn = get_connection()
    print("✅ Connected to PostgreSQL")

    max_iterations = env_int("GENERATOR_MAX_ITERATIONS", 0)  # 0 = unlimited

    iteration = 0
    try:
        while True:
            iteration += 1
            stats = run_iteration(conn)
            print(f"iteration {iteration:03d} -> {format_stats(stats)}")
            if args.once or (max_iterations > 0 and iteration >= max_iterations):
                print(f"✅ Completed {iteration} iteration(s). Generator stopping cleanly.")
                break
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting cleanly.")
    finally:
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
