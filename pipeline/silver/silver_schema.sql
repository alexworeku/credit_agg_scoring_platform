CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id BIGINT NOT NULL,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    gender VARCHAR,
    age INT,
    income NUMERIC,
    employment_type VARCHAR,
    education VARCHAR,
    housing_type VARCHAR,
    account_open_date DATE,
    record_source VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (customer_id, institution_id)
);

CREATE TABLE IF NOT EXISTS silver.loans (
    loan_id VARCHAR PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    loan_type VARCHAR,
    loan_amount NUMERIC,
    loan_status VARCHAR,
    start_date DATE,
    end_date DATE,
    days_past_due INT,
    default_flag INT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.loan_payments (
    payment_id VARCHAR PRIMARY KEY,
    loan_id VARCHAR,
    customer_id BIGINT NOT NULL,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    payment_amount NUMERIC,
    scheduled_amount NUMERIC,
    payment_date DATE,
    days_late INT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.account_balances (
    account_id VARCHAR NOT NULL,
    customer_id BIGINT NOT NULL,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    balance NUMERIC,
    credit_limit NUMERIC,
    days_past_due INT,
    snapshot_date DATE,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (account_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    transaction_type VARCHAR NOT NULL,
    transaction_date DATE,
    amount NUMERIC,
    expected_amount NUMERIC,
    delta_amount NUMERIC,
    status VARCHAR NOT NULL,
    reference_id VARCHAR,
    days_past_due INT,
    created_at TIMESTAMP NOT NULL
);
