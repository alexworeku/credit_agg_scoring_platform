from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
from sqlalchemy import create_engine, text


class RepositoryError(RuntimeError):
    """Raised when repository data cannot be loaded or queried."""


class BaseRepository:
    def startup(self) -> None:
        raise NotImplementedError

    def health(self) -> dict[str, Any]:
        raise NotImplementedError

    def search_customers(self, query: str | None, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_profile(self, customer_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_score(self, customer_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_features(self, customer_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_score_history(self, customer_id: int, limit: int = 12) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_transactions(
        self,
        customer_id: int,
        limit: int,
        transaction_type: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError


def _data_path(path: str) -> Path:
    value = Path(path)
    return value if value.is_absolute() else Path.cwd() / value


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


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return None


class ParquetRepository(BaseRepository):
    def __init__(
        self,
        customers_path: str = "data/medallion/silver/customers",
        transactions_path: str = "data/medallion/silver/transactions",
        features_path: str = "data/medallion/gold/customer_credit_features",
        scores_path: str = "data/medallion/gold/customer_credit_scores",
    ) -> None:
        self.customers_path = _data_path(customers_path)
        self.transactions_path = _data_path(transactions_path)
        self.features_path = _data_path(features_path)
        self.scores_path = _data_path(scores_path)

        self._customers: dict[int, dict[str, Any]] = {}
        self._scores: dict[int, dict[str, Any]] = {}
        self._features: dict[int, dict[str, Any]] = {}
        self._ranked_customer_ids: list[int] = []
        self._scores_ds: ds.Dataset | None = None
        self._transactions_ds: ds.Dataset | None = None

    @staticmethod
    def _dataset(path: Path) -> ds.Dataset:
        if not path.exists():
            raise RepositoryError(f"Missing parquet dataset path: {path}")
        return ds.dataset(str(path), format="parquet")

    @staticmethod
    def _load_map(dataset: ds.Dataset, columns: list[str]) -> dict[int, dict[str, Any]]:
        table = dataset.to_table(columns=columns)
        records: dict[int, dict[str, Any]] = {}
        for row in table.to_pylist():
            customer_id = int(row["customer_id"])
            records[customer_id] = {k: v for k, v in row.items() if k != "customer_id"}
        return records

    def startup(self) -> None:
        customers_ds = self._dataset(self.customers_path)
        scores_ds = self._dataset(self.scores_path)
        features_ds = self._dataset(self.features_path)
        self._scores_ds = scores_ds
        self._transactions_ds = self._dataset(self.transactions_path)

        self._customers = self._load_map(
            customers_ds,
            [
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
        )
        self._scores = self._load_map(
            scores_ds,
            [
                "customer_id",
                "credit_score",
                "risk_level",
                "default_probability",
                "score_generated_at",
            ],
        )
        self._features = self._load_map(
            features_ds,
            [
                "customer_id",
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
            ],
        )

        self._ranked_customer_ids = sorted(
            self._scores.keys(),
            key=lambda customer_id: int(self._scores[customer_id]["credit_score"]),
            reverse=True,
        )

    def health(self) -> dict[str, Any]:
        return {
            "mode": "parquet",
            "loaded_customers": len(self._customers),
            "loaded_scores": len(self._scores),
            "loaded_features": len(self._features),
        }

    def search_customers(self, query: str | None, limit: int) -> list[dict[str, Any]]:
        normalized_query = query.strip() if query else None
        items: list[dict[str, Any]] = []
        for customer_id in self._ranked_customer_ids:
            if normalized_query and normalized_query not in str(customer_id):
                continue
            profile = self._customers.get(customer_id)
            features = self._features.get(customer_id)
            score = self._scores.get(customer_id)
            if profile is None or features is None or score is None:
                continue
            items.append(
                {
                    "customer_id": customer_id,
                    "age": _to_int(profile.get("age")),
                    "income": _to_float(profile.get("income")),
                    "employment_type": profile.get("employment_type"),
                    "credit_score": _to_int(score.get("credit_score")) or 0,
                    "risk_level": str(score.get("risk_level") or "UNKNOWN"),
                    "default_probability": _to_float(score.get("default_probability")) or 0.0,
                }
            )
            if len(items) >= limit:
                break
        return items

    def get_profile(self, customer_id: int) -> dict[str, Any] | None:
        record = self._customers.get(customer_id)
        if record is None:
            return None
        return {
            "customer_id": customer_id,
            "institution_id": record.get("institution_id"),
            "source_system": record.get("source_system"),
            "gender": record.get("gender"),
            "age": _to_int(record.get("age")),
            "income": _to_float(record.get("income")),
            "employment_type": record.get("employment_type"),
            "education": record.get("education"),
            "housing_type": record.get("housing_type"),
            "account_open_date": _to_date(record.get("account_open_date")),
        }

    def get_score(self, customer_id: int) -> dict[str, Any] | None:
        history = self.get_score_history(customer_id=customer_id, limit=1)
        return history[-1] if history else None

    def get_features(self, customer_id: int) -> dict[str, Any] | None:
        return self._features.get(customer_id)

    def get_score_history(self, customer_id: int, limit: int = 12) -> list[dict[str, Any]]:
        if self._scores_ds is None:
            raise RepositoryError("Parquet repository not initialized. Call startup() first.")

        table = self._scores_ds.to_table(
            columns=["customer_id", "credit_score", "risk_level", "default_probability", "score_generated_at"],
            filter=ds.field("customer_id") == customer_id,
        )
        rows = table.to_pylist()
        rows.sort(key=lambda item: item.get("score_generated_at") or date.min)
        if limit > 0:
            rows = rows[-limit:]
        return [
            {
                "credit_score": _to_int(row.get("credit_score")) or 0,
                "risk_level": str(row.get("risk_level") or "UNKNOWN"),
                "default_probability": _to_float(row.get("default_probability")) or 0.0,
                "score_generated_at": row.get("score_generated_at"),
            }
            for row in rows
        ]

    def get_transactions(
        self,
        customer_id: int,
        limit: int,
        transaction_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._transactions_ds is None:
            raise RepositoryError("Parquet repository not initialized. Call startup() first.")

        filter_expr = ds.field("customer_id") == customer_id
        if transaction_type:
            filter_expr = filter_expr & (ds.field("transaction_type") == transaction_type.upper())

        table = self._transactions_ds.to_table(
            columns=[
                "transaction_id",
                "customer_id",
                "transaction_type",
                "transaction_date",
                "amount",
                "expected_amount",
                "delta_amount",
                "status",
                "reference_id",
                "days_past_due",
            ],
            filter=filter_expr,
        )
        rows = [
            {
                "transaction_id": str(row.get("transaction_id")),
                "customer_id": customer_id,
                "transaction_type": str(row.get("transaction_type") or "UNKNOWN"),
                "transaction_date": _to_date(row.get("transaction_date")),
                "amount": _to_float(row.get("amount")),
                "expected_amount": _to_float(row.get("expected_amount")),
                "delta_amount": _to_float(row.get("delta_amount")),
                "status": str(row.get("status") or "UNKNOWN"),
                "reference_id": row.get("reference_id"),
                "days_past_due": _to_int(row.get("days_past_due")),
            }
            for row in table.to_pylist()
        ]
        rows.sort(key=lambda item: item.get("transaction_date") or date.min, reverse=True)
        return rows[:limit]


class SqlRepository(BaseRepository):
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.engine = create_engine(database_url, future=True)

    def startup(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def health(self) -> dict[str, Any]:
        with self.engine.connect() as conn:
            customers = conn.execute(text("SELECT COUNT(*) FROM customer_profile")).scalar_one()
            scores = conn.execute(text("SELECT COUNT(*) FROM customer_credit_scores")).scalar_one()
            features = conn.execute(text("SELECT COUNT(*) FROM customer_credit_features")).scalar_one()
        return {
            "mode": "operational_db",
            "database_url": self.database_url,
            "loaded_customers": int(customers),
            "loaded_scores": int(scores),
            "loaded_features": int(features),
        }

    def search_customers(self, query: str | None, limit: int) -> list[dict[str, Any]]:
        pattern = f"%{query.strip()}%" if query else None
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        p.customer_id,
                        p.age,
                        p.income,
                        p.employment_type,
                        s.credit_score,
                        s.risk_level,
                        s.default_probability
                    FROM customer_profile p
                    JOIN customer_credit_scores s
                        ON s.customer_id = p.customer_id
                    WHERE (:pattern IS NULL OR CAST(p.customer_id AS VARCHAR) LIKE :pattern)
                    ORDER BY s.credit_score DESC, p.customer_id
                    LIMIT :limit
                    """
                ),
                {"pattern": pattern, "limit": limit},
            ).mappings()
            return [dict(row) for row in rows]

    def get_profile(self, customer_id: int) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM customer_profile WHERE customer_id = :customer_id"),
                {"customer_id": customer_id},
            ).mappings().first()
            return dict(row) if row else None

    def get_score(self, customer_id: int) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        credit_score,
                        risk_level,
                        default_probability,
                        score_generated_at
                    FROM customer_credit_scores
                    WHERE customer_id = :customer_id
                    ORDER BY
                        CASE WHEN score_generated_at IS NULL THEN 1 ELSE 0 END ASC,
                        score_generated_at DESC
                    LIMIT 1
                    """
                ),
                {"customer_id": customer_id},
            ).mappings().first()
            return dict(row) if row else None

    def get_score_history(self, customer_id: int, limit: int = 12) -> list[dict[str, Any]]:
        history_query = text(
            """
            SELECT
                credit_score,
                risk_level,
                default_probability,
                score_generated_at
            FROM customer_credit_score_history
            WHERE customer_id = :customer_id
            ORDER BY
                CASE WHEN score_generated_at IS NULL THEN 1 ELSE 0 END ASC,
                score_generated_at DESC,
                history_id DESC
            LIMIT :limit
            """
        )
        fallback_query = text(
            """
            SELECT
                credit_score,
                risk_level,
                default_probability,
                score_generated_at
            FROM customer_credit_scores
            WHERE customer_id = :customer_id
            ORDER BY
                CASE WHEN score_generated_at IS NULL THEN 1 ELSE 0 END ASC,
                score_generated_at ASC
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            try:
                history_rows = [dict(row) for row in conn.execute(history_query, {"customer_id": customer_id, "limit": limit}).mappings()]
                if history_rows:
                    history_rows.reverse()
                    return history_rows
            except Exception:
                pass
            rows = conn.execute(
                fallback_query,
                {"customer_id": customer_id, "limit": limit},
            ).mappings()
            return [dict(row) for row in rows]

    def get_features(self, customer_id: int) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        total_accounts,
                        account_age_days,
                        avg_balance,
                        max_balance,
                        min_balance,
                        total_loans,
                        active_loans,
                        closed_loans,
                        total_payments,
                        late_payments,
                        late_payment_ratio,
                        max_days_past_due,
                        avg_days_past_due,
                        total_debt,
                        debt_to_income_ratio,
                        credit_utilization,
                        account_stability
                    FROM customer_credit_features
                    WHERE customer_id = :customer_id
                    """
                ),
                {"customer_id": customer_id},
            ).mappings().first()
            return dict(row) if row else None

    def get_transactions(
        self,
        customer_id: int,
        limit: int,
        transaction_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if transaction_type:
            query = text(
                """
                SELECT
                    transaction_id,
                    customer_id,
                    transaction_type,
                    transaction_date,
                    amount,
                    expected_amount,
                    delta_amount,
                    status,
                    reference_id,
                    days_past_due
                FROM customer_transactions
                WHERE customer_id = :customer_id
                  AND transaction_type = :transaction_type
                ORDER BY transaction_date DESC, transaction_id DESC
                LIMIT :limit
                """
            )
            parameters = {
                "customer_id": customer_id,
                "transaction_type": transaction_type.upper(),
                "limit": limit,
            }
        else:
            query = text(
                """
                SELECT
                    transaction_id,
                    customer_id,
                    transaction_type,
                    transaction_date,
                    amount,
                    expected_amount,
                    delta_amount,
                    status,
                    reference_id,
                    days_past_due
                FROM customer_transactions
                WHERE customer_id = :customer_id
                ORDER BY transaction_date DESC, transaction_id DESC
                LIMIT :limit
                """
            )
            parameters = {"customer_id": customer_id, "limit": limit}

        with self.engine.connect() as conn:
            rows = conn.execute(query, parameters).mappings()
            return [dict(row) for row in rows]
