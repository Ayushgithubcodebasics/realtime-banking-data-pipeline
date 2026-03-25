-- ============================================================
-- Banking OLTP schema (PostgreSQL 16)
-- Designed to produce realistic CDC events for a portfolio-grade
-- data engineering project.
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(100)        NOT NULL,
    last_name   VARCHAR(100)        NOT NULL,
    email       VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS accounts (
    id           SERIAL PRIMARY KEY,
    customer_id  INT            NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
    account_type VARCHAR(50)    NOT NULL,
    balance      NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    currency     CHAR(3)        NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          INT            NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    txn_type            VARCHAR(50)    NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    related_account_id  INT            NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    status              VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED',
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at
    ON customers(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_accounts_customer
    ON accounts(customer_id);

CREATE INDEX IF NOT EXISTS idx_accounts_updated_at
    ON accounts(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_account_created
    ON transactions(account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_type
    ON transactions(txn_type);

CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_set_updated_at ON customers;
CREATE TRIGGER trg_customers_set_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

DROP TRIGGER IF EXISTS trg_accounts_set_updated_at ON accounts;
CREATE TRIGGER trg_accounts_set_updated_at
BEFORE UPDATE ON accounts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();

SELECT pg_create_logical_replication_slot('banking_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'banking_slot'
);
