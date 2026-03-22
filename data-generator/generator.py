"""
Banking Data Generator
Continuously inserts fake customers, accounts, and transactions into PostgreSQL.
Debezium CDC will stream these changes to Kafka automatically.

Usage:
    python generator.py           # runs in a loop every SLEEP_SECONDS
    python generator.py --once    # one iteration and exit (useful for CI tests)
"""

import argparse
import os
import random
import sys
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load .env relative to this script (works regardless of working directory)
load_dotenv(Path(__file__).parent / ".env")

# ── Configuration ─────────────────────────────────────────────
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1_000.00
CURRENCY = "USD"
INITIAL_BALANCE_MIN = Decimal("100.00")
INITIAL_BALANCE_MAX = Decimal("10_000.00")
SLEEP_SECONDS = 5

# ── CLI ───────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Banking data generator")
parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
args = parser.parse_args()
LOOP = not args.once

# ── Faker ─────────────────────────────────────────────────────
fake = Faker()


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# ── Database connection ───────────────────────────────────────
def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "banking"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


# ── Core generation logic ─────────────────────────────────────
def run_iteration(conn: psycopg2.extensions.connection) -> None:
    """One full iteration: insert customers → accounts → transactions."""
    with conn:  # creates a transaction; commits on exit, rolls back on exception
        with conn.cursor() as cur:
            # 1. Customers
            customer_ids: list[int] = []
            for _ in range(NUM_CUSTOMERS):
                cur.execute(
                    """
                    INSERT INTO customers (first_name, last_name, email)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (fake.first_name(), fake.last_name(), fake.unique.email()),
                )
                customer_ids.append(cur.fetchone()[0])

            # 2. Accounts (2 per customer)
            account_ids: list[int] = []
            for cust_id in customer_ids:
                for _ in range(ACCOUNTS_PER_CUSTOMER):
                    cur.execute(
                        """
                        INSERT INTO accounts
                            (customer_id, account_type, balance, currency)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            cust_id,
                            random.choice(["SAVINGS", "CHECKING"]),
                            random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
                            CURRENCY,
                        ),
                    )
                    account_ids.append(cur.fetchone()[0])

            # 3. Transactions
            txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
            for _ in range(NUM_TRANSACTIONS):
                acct = random.choice(account_ids)
                txn_type = random.choice(txn_types)
                related = None
                if txn_type == "TRANSFER" and len(account_ids) > 1:
                    related = random.choice([a for a in account_ids if a != acct])

                cur.execute(
                    """
                    INSERT INTO transactions
                        (account_id, txn_type, amount, related_account_id, status)
                    VALUES (%s, %s, %s, %s, 'COMPLETED')
                    """,
                    (
                        acct,
                        txn_type,
                        round(random.uniform(1.0, MAX_TXN_AMOUNT), 2),
                        related,
                    ),
                )

    print(
        f"  ✅ Inserted {NUM_CUSTOMERS} customers · "
        f"{NUM_CUSTOMERS * ACCOUNTS_PER_CUSTOMER} accounts · "
        f"{NUM_TRANSACTIONS} transactions"
    )


# ── Main ──────────────────────────────────────────────────────
def main() -> None:
    print("🏦 Banking data generator starting …")
    print(
        f"   Host : {os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"  DB: {os.getenv('POSTGRES_DB')}"
    )

    conn = get_connection()
    print("✅ Connected to PostgreSQL")

    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\n── Iteration {iteration} ──────────────────────")
            run_iteration(conn)
            if not LOOP:
                break
            print(f"   Sleeping {SLEEP_SECONDS}s …")
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Exiting gracefully …")
    finally:
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
