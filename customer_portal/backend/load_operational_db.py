from __future__ import annotations

import argparse
from datetime import date
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load customer-facing profile, score, feature, and transaction datasets into an operational database.",
    )
    parser.add_argument(
        "--database-url",
        default="sqlite:///customer_portal.db",
        help="SQLAlchemy DB URL (example: postgresql+psycopg://user:pass@host:5432/dbname)",
    )
    parser.add_argument("--customers-path", default="data/medallion/silver/customers")
    parser.add_argument("--loans-path", default="data/medallion/silver/loans")
    parser.add_argument("--transactions-path", default="data/medallion/silver/transactions")
    parser.add_argument("--features-path", default="data/medallion/gold/customer_credit_features")
    parser.add_argument("--scores-path", default="data/medallion/gold/customer_credit_scores")
    parser.add_argument("--replace", action="store_true", help="Drop and recreate tables before loading")
    parser.add_argument("--batch-size", type=int, default=20000, help="Batch size for inserts")
    parser.add_argument(
        "--transaction-lookback-days",
        type=int,
        default=365 * 3,
        help="Only load transactions on/after (latest score_generated_at - lookback days). Use 0 to disable.",
    )
    parser.add_argument(
        "--disable-sqlite-fast-mode",
        action="store_true",
        help="Disable SQLite bulk-load PRAGMAs.",
    )
    return parser.parse_args()


def _path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else Path.cwd() / value


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _latest_score_date(scores_dataset: ds.Dataset) -> date:
    table = scores_dataset.to_table(columns=["score_generated_at"])
    latest = None
    for value in table.column("score_generated_at").to_pylist():
        if value is None:
            continue
        candidate = value.date() if hasattr(value, "date") else None
        if candidate is None:
            continue
        latest = candidate if latest is None or candidate > latest else latest
    return latest or date.today()


def build_tables(metadata: MetaData) -> dict[str, Table]:
    customer_profile = Table(
        "customer_profile",
        metadata,
        Column("customer_id", BigInteger, primary_key=True),
        Column("institution_id", String(64), nullable=True),
        Column("source_system", String(64), nullable=True),
        Column("gender", String(32), nullable=True),
        Column("age", Integer, nullable=True),
        Column("income", Float, nullable=True),
        Column("employment_type", String(128), nullable=True),
        Column("education", String(128), nullable=True),
        Column("housing_type", String(128), nullable=True),
        Column("account_open_date", Date, nullable=True),
    )

    customer_credit_scores = Table(
        "customer_credit_scores",
        metadata,
        Column("customer_id", BigInteger, primary_key=True),
        Column("institution_id", String(64), nullable=True),
        Column("source_system", String(64), nullable=True),
        Column("credit_score", Integer, nullable=False),
        Column("default_probability", Float, nullable=False),
        Column("risk_level", String(32), nullable=False),
        Column("score_generated_at", DateTime, nullable=True),
    )

    customer_credit_features = Table(
        "customer_credit_features",
        metadata,
        Column("customer_id", BigInteger, primary_key=True),
        Column("total_accounts", Integer, nullable=True),
        Column("account_age_days", Integer, nullable=True),
        Column("avg_balance", Float, nullable=True),
        Column("max_balance", Float, nullable=True),
        Column("min_balance", Float, nullable=True),
        Column("total_loans", Integer, nullable=True),
        Column("active_loans", Integer, nullable=True),
        Column("closed_loans", Integer, nullable=True),
        Column("total_payments", Integer, nullable=True),
        Column("late_payments", Integer, nullable=True),
        Column("late_payment_ratio", Float, nullable=True),
        Column("max_days_past_due", Integer, nullable=True),
        Column("avg_days_past_due", Float, nullable=True),
        Column("total_debt", Float, nullable=True),
        Column("debt_to_income_ratio", Float, nullable=True),
        Column("credit_utilization", Float, nullable=True),
        Column("account_stability", Float, nullable=True),
    )

    customer_loans = Table(
        "customer_loans",
        metadata,
        Column("loan_id", String(256), primary_key=True),
        Column("customer_id", BigInteger, nullable=False, index=True),
        Column("institution_id", String(64), nullable=True),
        Column("source_system", String(64), nullable=True),
        Column("loan_type", String(128), nullable=True),
        Column("loan_amount", Float, nullable=True),
        Column("loan_status", String(64), nullable=True),
        Column("start_date", Date, nullable=True),
        Column("end_date", Date, nullable=True),
        Column("days_past_due", Integer, nullable=True),
        Column("default_flag", Integer, nullable=True),
    )

    customer_transactions = Table(
        "customer_transactions",
        metadata,
        Column("transaction_id", String(256), primary_key=True),
        Column("customer_id", BigInteger, nullable=False, index=True),
        Column("institution_id", String(64), nullable=True),
        Column("source_system", String(64), nullable=True),
        Column("transaction_type", String(64), nullable=False, index=True),
        Column("transaction_date", Date, nullable=True, index=True),
        Column("amount", Float, nullable=True),
        Column("expected_amount", Float, nullable=True),
        Column("delta_amount", Float, nullable=True),
        Column("status", String(32), nullable=False),
        Column("reference_id", String(128), nullable=True),
        Column("days_past_due", Integer, nullable=True),
    )

    return {
        "customer_profile": customer_profile,
        "customer_credit_scores": customer_credit_scores,
        "customer_credit_features": customer_credit_features,
        "customer_loans": customer_loans,
        "customer_transactions": customer_transactions,
    }


def _insert_batches(connection, table: Table, rows: list[dict], batch_size: int) -> int:
    inserted = 0
    if not rows:
        return inserted
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        connection.execute(table.insert(), batch)
        inserted += len(batch)
    return inserted


def load_customer_profile(connection, table: Table, dataset: ds.Dataset, batch_size: int) -> int:
    seen_customer_ids: set[int] = set()
    inserted = 0
    for batch in dataset.to_batches(
        columns=[
            "customer_id",
            "institution_id",
            "source_system",
            "gender",
            "age",
            "income",
            "employment_type",
            "education",
            "housing_type",
            "account_open_date",
        ],
        batch_size=batch_size,
    ):
        rows = []
        for row in batch.to_pylist():
            customer_id = int(row["customer_id"])
            if customer_id in seen_customer_ids:
                continue
            seen_customer_ids.add(customer_id)
            rows.append(
                {
                    "customer_id": customer_id,
                    "institution_id": row.get("institution_id"),
                    "source_system": row.get("source_system"),
                    "gender": row.get("gender"),
                    "age": _to_int(row.get("age")),
                    "income": _to_float(row.get("income")),
                    "employment_type": row.get("employment_type"),
                    "education": row.get("education"),
                    "housing_type": row.get("housing_type"),
                    "account_open_date": row.get("account_open_date"),
                }
            )
        inserted += _insert_batches(connection, table, rows, batch_size=batch_size)
    return inserted


def load_customer_scores(connection, table: Table, dataset: ds.Dataset, batch_size: int) -> int:
    inserted = 0
    for batch in dataset.to_batches(
        columns=[
            "customer_id",
            "institution_id",
            "source_system",
            "credit_score",
            "default_probability",
            "risk_level",
            "score_generated_at",
        ],
        batch_size=batch_size,
    ):
        rows = []
        for row in batch.to_pylist():
            rows.append(
                {
                    "customer_id": int(row["customer_id"]),
                    "institution_id": row.get("institution_id"),
                    "source_system": row.get("source_system"),
                    "credit_score": int(row["credit_score"]),
                    "default_probability": float(row["default_probability"]),
                    "risk_level": str(row["risk_level"]),
                    "score_generated_at": row.get("score_generated_at"),
                }
            )
        inserted += _insert_batches(connection, table, rows, batch_size=batch_size)
    return inserted


def load_customer_features(connection, table: Table, dataset: ds.Dataset, batch_size: int) -> int:
    inserted = 0
    feature_columns = [
        "total_accounts",
        "account_age_days",
        "avg_balance",
        "max_balance",
        "min_balance",
        "total_loans",
        "active_loans",
        "closed_loans",
        "total_payments",
        "late_payments",
        "late_payment_ratio",
        "max_days_past_due",
        "avg_days_past_due",
        "total_debt",
        "debt_to_income_ratio",
        "credit_utilization",
        "account_stability",
    ]
    for batch in dataset.to_batches(columns=["customer_id", *feature_columns], batch_size=batch_size):
        rows = []
        for row in batch.to_pylist():
            transformed = {"customer_id": int(row["customer_id"])}
            for col in feature_columns:
                value = row.get(col)
                transformed[col] = _to_float(value) if isinstance(value, float) else value
            rows.append(transformed)
        inserted += _insert_batches(connection, table, rows, batch_size=batch_size)
    return inserted


def load_customer_loans(connection, table: Table, dataset: ds.Dataset, batch_size: int) -> int:
    inserted = 0
    for batch in dataset.to_batches(
        columns=[
            "loan_id",
            "customer_id",
            "institution_id",
            "source_system",
            "loan_type",
            "loan_amount",
            "loan_status",
            "start_date",
            "end_date",
            "days_past_due",
            "default_flag",
        ],
        batch_size=batch_size,
    ):
        rows = []
        for row in batch.to_pylist():
            rows.append(
                {
                    "loan_id": str(row["loan_id"]),
                    "customer_id": int(row["customer_id"]),
                    "institution_id": row.get("institution_id"),
                    "source_system": row.get("source_system"),
                    "loan_type": row.get("loan_type"),
                    "loan_amount": _to_float(row.get("loan_amount")),
                    "loan_status": row.get("loan_status"),
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "days_past_due": _to_int(row.get("days_past_due")),
                    "default_flag": _to_int(row.get("default_flag")),
                }
            )
        inserted += _insert_batches(connection, table, rows, batch_size=batch_size)
    return inserted


def load_transactions(
    connection,
    table: Table,
    dataset: ds.Dataset,
    batch_size: int,
    cutoff_date: date | None,
    commit_every_batches: int = 0,
) -> int:
    inserted = 0
    batches_since_commit = 0
    for batch in dataset.to_batches(
        columns=[
            "transaction_id",
            "customer_id",
            "institution_id",
            "source_system",
            "transaction_type",
            "transaction_date",
            "amount",
            "expected_amount",
            "delta_amount",
            "status",
            "reference_id",
            "days_past_due",
        ],
        batch_size=batch_size,
    ):
        rows = []
        for row in batch.to_pylist():
            transaction_date = row.get("transaction_date")
            if cutoff_date is not None and transaction_date is not None and transaction_date < cutoff_date:
                continue

            rows.append(
                {
                    "transaction_id": str(row["transaction_id"]),
                    "customer_id": int(row["customer_id"]),
                    "institution_id": row.get("institution_id"),
                    "source_system": row.get("source_system"),
                    "transaction_type": row.get("transaction_type"),
                    "transaction_date": transaction_date,
                    "amount": _to_float(row.get("amount")),
                    "expected_amount": _to_float(row.get("expected_amount")),
                    "delta_amount": _to_float(row.get("delta_amount")),
                    "status": row.get("status"),
                    "reference_id": row.get("reference_id"),
                    "days_past_due": _to_int(row.get("days_past_due")),
                }
            )
        inserted += _insert_batches(connection, table, rows, batch_size=batch_size)
        if commit_every_batches > 0:
            batches_since_commit += 1
            if batches_since_commit >= commit_every_batches:
                connection.commit()
                batches_since_commit = 0
    if commit_every_batches > 0 and batches_since_commit > 0:
        connection.commit()
    return inserted


def main() -> None:
    args = parse_args()
    metadata = MetaData()
    tables = build_tables(metadata)
    engine = create_engine(args.database_url, future=True)

    customers_dataset = ds.dataset(str(_path(args.customers_path)), format="parquet")
    loans_dataset = ds.dataset(str(_path(args.loans_path)), format="parquet")
    transactions_dataset = ds.dataset(str(_path(args.transactions_path)), format="parquet")
    features_dataset = ds.dataset(str(_path(args.features_path)), format="parquet")
    scores_dataset = ds.dataset(str(_path(args.scores_path)), format="parquet")

    latest_score_date = _latest_score_date(scores_dataset)
    cutoff_date = None
    if args.transaction_lookback_days > 0:
        cutoff_date = latest_score_date - timedelta(days=args.transaction_lookback_days)

    if args.replace:
        metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine, checkfirst=True)

    with engine.begin() as connection:
        if engine.dialect.name == "sqlite" and not args.disable_sqlite_fast_mode:
            connection.execute(text("PRAGMA journal_mode=WAL"))
            connection.execute(text("PRAGMA synchronous=OFF"))
            connection.execute(text("PRAGMA temp_store=MEMORY"))
            connection.execute(text("PRAGMA cache_size=-200000"))

    if args.replace:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM customer_transactions"))
            connection.execute(text("DELETE FROM customer_loans"))
            connection.execute(text("DELETE FROM customer_credit_features"))
            connection.execute(text("DELETE FROM customer_credit_scores"))
            connection.execute(text("DELETE FROM customer_profile"))

    with engine.begin() as connection:
        customer_count = load_customer_profile(connection, tables["customer_profile"], customers_dataset, args.batch_size)
    with engine.begin() as connection:
        loan_count = load_customer_loans(
            connection,
            tables["customer_loans"],
            loans_dataset,
            args.batch_size,
        )
    with engine.begin() as connection:
        score_count = load_customer_scores(connection, tables["customer_credit_scores"], scores_dataset, args.batch_size)
    with engine.begin() as connection:
        feature_count = load_customer_features(connection, tables["customer_credit_features"], features_dataset, args.batch_size)
    with engine.connect() as connection:
        transaction_count = load_transactions(
            connection,
            tables["customer_transactions"],
            transactions_dataset,
            args.batch_size,
            cutoff_date=cutoff_date,
            commit_every_batches=25 if engine.dialect.name == "postgresql" else 0,
        )
        connection.commit()

    print(f"Latest score date detected: {latest_score_date}")
    print(f"Transaction cutoff date: {cutoff_date if cutoff_date else 'disabled'}")
    print(f"Loaded customer_profile rows: {customer_count}")
    print(f"Loaded customer_loans rows: {loan_count}")
    print(f"Loaded customer_credit_scores rows: {score_count}")
    print(f"Loaded customer_credit_features rows: {feature_count}")
    print(f"Loaded customer_transactions rows: {transaction_count}")
    print(f"Operational DB ready at: {args.database_url}")


if __name__ == "__main__":
    main()
