from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pipeline.common.config import settings

TRANSACTION_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("customer_id", pa.int64()),
        ("institution_id", pa.string()),
        ("source_system", pa.string()),
        ("transaction_type", pa.string()),
        ("transaction_date", pa.date32()),
        ("amount", pa.float64()),
        ("expected_amount", pa.float64()),
        ("delta_amount", pa.float64()),
        ("status", pa.string()),
        ("reference_id", pa.string()),
        ("days_past_due", pa.int32()),
        ("created_at", pa.timestamp("us")),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill canonical Silver transactions from existing Silver tables.")
    parser.add_argument("--loan-payments-path", default=str(settings.silver_dir / "loan_payments"))
    parser.add_argument("--account-balances-path", default=str(settings.silver_dir / "account_balances"))
    parser.add_argument("--output-path", default=str(settings.silver_dir / "transactions"))
    parser.add_argument("--batch-size", type=int, default=50000)
    return parser.parse_args()


def _path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else Path.cwd() / value


def _write_batches(dataset: ds.Dataset, columns: list[str], mapper, output_dir: Path, prefix: str, batch_size: int) -> int:
    count = 0
    for batch_index, batch in enumerate(dataset.to_batches(columns=columns, batch_size=batch_size)):
        rows = [mapper(row) for row in batch.to_pylist()]
        if not rows:
            continue
        table = pa.Table.from_pylist(rows, schema=TRANSACTION_SCHEMA)
        pq.write_table(table, output_dir / f"{prefix}-{batch_index:05d}.parquet")
        count += len(rows)
    return count


def main() -> None:
    args = parse_args()
    loan_payments_path = _path(args.loan_payments_path)
    account_balances_path = _path(args.account_balances_path)
    output_path = _path(args.output_path)

    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    loan_payments = ds.dataset(str(loan_payments_path), format="parquet")
    account_balances = ds.dataset(str(account_balances_path), format="parquet")

    payment_count = _write_batches(
        loan_payments,
        columns=[
            "payment_id",
            "customer_id",
            "institution_id",
            "source_system",
            "loan_id",
            "payment_amount",
            "scheduled_amount",
            "payment_date",
            "days_late",
        ],
        mapper=lambda row: {
            "transaction_id": str(row["payment_id"]),
            "customer_id": int(row["customer_id"]),
            "institution_id": row.get("institution_id"),
            "source_system": row.get("source_system"),
            "transaction_type": "LOAN_PAYMENT",
            "transaction_date": row.get("payment_date"),
            "amount": float(row["payment_amount"]) if row.get("payment_amount") is not None else None,
            "expected_amount": float(row["scheduled_amount"]) if row.get("scheduled_amount") is not None else None,
            "delta_amount": (
                float(row["payment_amount"]) - float(row["scheduled_amount"])
                if row.get("payment_amount") is not None and row.get("scheduled_amount") is not None
                else None
            ),
            "status": "LATE" if int(row.get("days_late") or 0) > 0 else "ON_TIME",
            "reference_id": row.get("loan_id"),
            "days_past_due": int(row.get("days_late") or 0),
            "created_at": row.get("created_at"),
        },
        output_dir=output_path,
        prefix="loan-payment",
        batch_size=args.batch_size,
    )

    balance_count = _write_batches(
        account_balances,
        columns=[
            "account_id",
            "customer_id",
            "institution_id",
            "source_system",
            "balance",
            "credit_limit",
            "snapshot_date",
            "days_past_due",
            "created_at",
        ],
        mapper=lambda row: {
            "transaction_id": f"BAL::{row['account_id']}::{row.get('snapshot_date')}" if row.get("snapshot_date") else f"BAL::{row['account_id']}",
            "customer_id": int(row["customer_id"]),
            "institution_id": row.get("institution_id"),
            "source_system": row.get("source_system"),
            "transaction_type": "BALANCE_SNAPSHOT",
            "transaction_date": row.get("snapshot_date"),
            "amount": float(row["balance"]) if row.get("balance") is not None else None,
            "expected_amount": float(row["credit_limit"]) if row.get("credit_limit") is not None else None,
            "delta_amount": None,
            "status": "PAST_DUE" if int(row.get("days_past_due") or 0) > 0 else "CURRENT",
            "reference_id": row.get("account_id"),
            "days_past_due": int(row.get("days_past_due") or 0),
            "created_at": row.get("created_at"),
        },
        output_dir=output_path,
        prefix="balance",
        batch_size=args.batch_size,
    )

    print(f"Wrote canonical transactions to {output_path}")
    print(f"Loan payment transactions: {payment_count}")
    print(f"Balance snapshot transactions: {balance_count}")


if __name__ == "__main__":
    main()
