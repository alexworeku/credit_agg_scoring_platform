CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.customer_credit_features (
    customer_id BIGINT PRIMARY KEY,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    total_accounts INT,
    account_age_days INT,
    avg_balance NUMERIC,
    max_balance NUMERIC,
    min_balance NUMERIC,
    total_loans INT,
    active_loans INT,
    closed_loans INT,
    total_payments INT,
    late_payments INT,
    late_payment_ratio NUMERIC,
    max_days_past_due INT,
    avg_days_past_due NUMERIC,
    total_debt NUMERIC,
    debt_to_income_ratio NUMERIC,
    credit_utilization NUMERIC,
    account_stability NUMERIC,
    default_flag INT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.customer_credit_scores (
    customer_id BIGINT PRIMARY KEY,
    institution_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    credit_score INT NOT NULL,
    default_probability NUMERIC NOT NULL,
    risk_level VARCHAR NOT NULL,
    score_generated_at TIMESTAMP NOT NULL
);
