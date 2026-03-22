-- ============================================================
-- Banking OLTP Schema (PostgreSQL 16)
-- This file runs automatically when the container first starts.
-- Logical replication is enabled via docker-compose.yml flags.
-- ============================================================

-- Customers: one row per bank customer
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(100)        NOT NULL,
    last_name   VARCHAR(100)        NOT NULL,
    email       VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

-- Accounts: each customer can have multiple accounts
CREATE TABLE IF NOT EXISTS accounts (
    id           SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    account_type VARCHAR(50)         NOT NULL,           -- SAVINGS | CHECKING
    balance      NUMERIC(18, 2)      NOT NULL DEFAULT 0 CHECK (balance >= 0),
    currency     CHAR(3)             NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

-- Transactions: immutable ledger of all money movements
CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    txn_type            VARCHAR(50)    NOT NULL,         -- DEPOSIT | WITHDRAWAL | TRANSFER
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    related_account_id  INT            NULL,             -- only for TRANSFER type
    status              VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED',
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_accounts_customer
    ON accounts(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_account_created
    ON transactions(account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_type
    ON transactions(txn_type);

-- Required for Debezium CDC: a publication on the tables we want to stream
-- Debezium will use this publication automatically when plugin.name=pgoutput
SELECT pg_create_logical_replication_slot('banking_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'banking_slot'
);
