CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.customers_raw (
    source_system VARCHAR NOT NULL,
    institution_id VARCHAR NOT NULL,
    customer_id BIGINT NOT NULL,
    record_source VARCHAR NOT NULL,
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.loans_raw (
    source_system VARCHAR NOT NULL,
    institution_id VARCHAR NOT NULL,
    customer_id BIGINT NOT NULL,
    loan_id VARCHAR NOT NULL,
    record_source VARCHAR NOT NULL,
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.loan_payments_raw (
    source_system VARCHAR NOT NULL,
    institution_id VARCHAR NOT NULL,
    customer_id BIGINT NOT NULL,
    loan_id VARCHAR NOT NULL,
    payment_id VARCHAR NOT NULL,
    record_source VARCHAR NOT NULL,
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.account_balances_raw (
    source_system VARCHAR NOT NULL,
    institution_id VARCHAR NOT NULL,
    customer_id BIGINT,
    account_id VARCHAR NOT NULL,
    record_source VARCHAR NOT NULL,
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);
