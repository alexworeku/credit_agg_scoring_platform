from __future__ import annotations

import json
from datetime import UTC, date, datetime
from uuid import uuid4

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
    Text,
    create_engine,
    text,
)

from customer_portal.backend.online_features import build_online_feature_row, feature_values_only
from customer_portal.backend.models import SimulationTransactionRequest
from pipeline.common.config import settings
from pipeline.ml.scoring_common import FEATURE_COLUMNS, ModelScorer


def _safe_datetime(value: object, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            try:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                return fallback
    return fallback

class RealtimeSimulationService:
    """Demo realtime ingestion path: Bronze -> Silver -> Gold -> scored operational snapshot."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.engine = create_engine(database_url, future=True)
        self.metadata = MetaData()
        self.scorer = ModelScorer.load(
            model_path=settings.artifacts_dir / "lightgbm_credit_model.pkl",
            metadata_path=settings.artifacts_dir / "model_metadata.json",
        )

        self.bronze_table = Table(
            "bronze_simulated_transactions",
            self.metadata,
            Column("simulation_id", String(64), primary_key=True),
            Column("customer_id", BigInteger, nullable=False, index=True),
            Column("institution_id", String(64), nullable=True),
            Column("source_system", String(64), nullable=True),
            Column("transaction_type", String(64), nullable=False),
            Column("transaction_date", Date, nullable=False),
            Column("amount", Float, nullable=False),
            Column("expected_amount", Float, nullable=True),
            Column("days_past_due", Integer, nullable=False, default=0),
            Column("status", String(32), nullable=False),
            Column("reference_id", String(128), nullable=True),
            Column("raw_json", Text, nullable=False),
            Column("ingested_at", DateTime, nullable=False),
        )

        self.silver_table = Table(
            "silver_simulated_transactions",
            self.metadata,
            Column("silver_event_id", String(72), primary_key=True),
            Column("simulation_id", String(64), nullable=False, index=True),
            Column("customer_id", BigInteger, nullable=False, index=True),
            Column("transaction_id", String(256), nullable=False, index=True),
            Column("transaction_type", String(64), nullable=False),
            Column("transaction_date", Date, nullable=False),
            Column("amount", Float, nullable=False),
            Column("expected_amount", Float, nullable=True),
            Column("delta_amount", Float, nullable=True),
            Column("days_past_due", Integer, nullable=False, default=0),
            Column("status", String(32), nullable=False),
            Column("reference_id", String(128), nullable=True),
            Column("processed_at", DateTime, nullable=False),
        )

        self.history_table = Table(
            "customer_credit_score_history",
            self.metadata,
            Column("history_id", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", BigInteger, nullable=False, index=True),
            Column("credit_score", Integer, nullable=False),
            Column("default_probability", Float, nullable=False),
            Column("risk_level", String(32), nullable=False),
            Column("score_generated_at", DateTime, nullable=False, index=True),
            Column("source", String(32), nullable=False, default="SIMULATION"),
        )

    def startup(self) -> None:
        self.metadata.create_all(self.engine, checkfirst=True)

    @staticmethod
    def _default_status(transaction_type: str, days_past_due: int) -> str:
        if transaction_type == "LOAN_PAYMENT":
            return "LATE" if days_past_due > 0 else "ON_TIME"
        return "PAST_DUE" if days_past_due > 0 else "CURRENT"

    @staticmethod
    def _build_raw_json(payload: SimulationTransactionRequest, status: str) -> str:
        transaction_date = payload.transaction_date.isoformat() if payload.transaction_date else None
        if payload.transaction_type == "LOAN_PAYMENT":
            raw = {
                "RECORD_SOURCE": "simulated_installments_payments",
                "AMT_PAYMENT": payload.amount,
                "AMT_INSTALMENT": payload.expected_amount,
                "STATUS": status,
                "DAYS_PAST_DUE": payload.days_past_due,
                "SIM_TRANSACTION_DATE": transaction_date,
            }
        else:
            raw = {
                "RECORD_SOURCE": "simulated_credit_balance",
                "AMT_BALANCE": payload.amount,
                "AMT_CREDIT_LIMIT_ACTUAL": payload.expected_amount,
                "SK_DPD": payload.days_past_due,
                "STATUS": status,
                "SIM_TRANSACTION_DATE": transaction_date,
            }
        if payload.raw_attributes:
            raw.update(payload.raw_attributes)
        return json.dumps(raw, default=str, separators=(",", ":"))

    def _normalize_payload(
        self,
        customer_id: int,
        payload: SimulationTransactionRequest,
        now: datetime,
    ) -> dict[str, object]:
        tx_date = payload.transaction_date or now.date()
        days_past_due = max(0, payload.days_past_due)
        status = (payload.status or self._default_status(payload.transaction_type, days_past_due)).upper()
        if payload.transaction_type == "LOAN_PAYMENT" and status not in {"ON_TIME", "LATE", "PAST_DUE"}:
            status = self._default_status(payload.transaction_type, days_past_due)
        if payload.transaction_type == "BALANCE_SNAPSHOT" and status not in {"CURRENT", "PAST_DUE"}:
            status = self._default_status(payload.transaction_type, days_past_due)

        expected_amount = payload.expected_amount
        if expected_amount is None:
            expected_amount = payload.amount if payload.transaction_type == "LOAN_PAYMENT" else max(payload.amount * 1.5, 1.0)

        reference_id = payload.reference_id or (
            f"SIM_LOAN_{customer_id}" if payload.transaction_type == "LOAN_PAYMENT" else f"SIM_ACC_{customer_id}"
        )
        return {
            "transaction_date": tx_date,
            "days_past_due": days_past_due,
            "status": status,
            "expected_amount": float(expected_amount),
            "reference_id": reference_id,
            "delta_amount": payload.amount - float(expected_amount),
        }

    @staticmethod
    def _load_customer_context(connection, customer_id: int) -> tuple[dict, dict, list[dict], list[dict]]:
        profile_row = connection.execute(
            text(
                """
                SELECT institution_id, source_system, income
                FROM customer_profile
                WHERE customer_id = :customer_id
                """
            ),
            {"customer_id": customer_id},
        ).mappings().first()
        if profile_row is None:
            raise ValueError(f"customer_id {customer_id} not found in customer_profile")

        score_row = connection.execute(
            text(
                """
                SELECT credit_score, default_probability, risk_level, score_generated_at
                FROM customer_credit_scores
                WHERE customer_id = :customer_id
                """
            ),
            {"customer_id": customer_id},
        ).mappings().first()
        if score_row is None:
            raise ValueError(f"customer_id {customer_id} not found in customer_credit_scores")

        loan_rows = connection.execute(
            text("SELECT * FROM customer_loans WHERE customer_id = :customer_id"),
            {"customer_id": customer_id},
        ).mappings().all()
        transaction_rows = connection.execute(
            text("SELECT * FROM customer_transactions WHERE customer_id = :customer_id"),
            {"customer_id": customer_id},
        ).mappings().all()

        return (
            dict(profile_row),
            dict(score_row),
            [dict(row) for row in loan_rows],
            [dict(row) for row in transaction_rows],
        )

    @staticmethod
    def _build_projected_transaction(
        customer_id: int,
        profile_row: dict,
        payload: SimulationTransactionRequest,
        normalized: dict[str, object],
        transaction_id: str,
    ) -> dict[str, object]:
        return {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "institution_id": profile_row.get("institution_id"),
            "source_system": profile_row.get("source_system"),
            "transaction_type": payload.transaction_type,
            "transaction_date": normalized["transaction_date"],
            "amount": payload.amount,
            "expected_amount": normalized["expected_amount"],
            "delta_amount": normalized["delta_amount"],
            "status": normalized["status"],
            "reference_id": normalized["reference_id"],
            "days_past_due": normalized["days_past_due"],
        }

    def _project_score(
        self,
        customer_id: int,
        profile_row: dict,
        score_row: dict,
        loan_rows: list[dict],
        transaction_rows: list[dict],
        projected_transaction: dict[str, object],
        as_of: datetime,
    ) -> tuple[dict[str, object], dict[str, float | int | str]]:
        new_features = build_online_feature_row(
            customer_id=customer_id,
            profile=profile_row,
            loans=loan_rows,
            transactions=[*transaction_rows, projected_transaction],
            as_of=as_of.date(),
        )
        scored = self.scorer.score_row(feature_values_only(new_features))
        previous_score = int(score_row["credit_score"])
        previous_probability = float(score_row["default_probability"])
        projected_score = int(scored["credit_score"])
        projected_probability = float(scored["default_probability"])
        projected_risk_level = str(scored["risk_level"])
        return (
            {
                "previous_credit_score": previous_score,
                "projected_credit_score": projected_score,
                "new_credit_score": projected_score,
                "score_change": projected_score - previous_score,
                "previous_default_probability": previous_probability,
                "projected_default_probability": projected_probability,
                "new_default_probability": projected_probability,
                "risk_level": projected_risk_level,
            },
            new_features,
        )

    def preview_transaction(self, customer_id: int, payload: SimulationTransactionRequest) -> dict:
        self.startup()
        now = datetime.now(UTC).replace(tzinfo=None)
        normalized = self._normalize_payload(customer_id=customer_id, payload=payload, now=now)
        projected_transaction = {
            "transaction_id": "WHATIF::PREVIEW",
        }

        with self.engine.connect() as connection:
            profile_row, score_row, loan_rows, transaction_rows = self._load_customer_context(
                connection=connection,
                customer_id=customer_id,
            )
            projected_transaction = self._build_projected_transaction(
                customer_id=customer_id,
                profile_row=profile_row,
                payload=payload,
                normalized=normalized,
                transaction_id="WHATIF::PREVIEW",
            )
            projection, _ = self._project_score(
                customer_id=customer_id,
                profile_row=profile_row,
                score_row=score_row,
                loan_rows=loan_rows,
                transaction_rows=transaction_rows,
                projected_transaction=projected_transaction,
                as_of=now,
            )

        return {
            "previous_credit_score": projection["previous_credit_score"],
            "projected_credit_score": projection["projected_credit_score"],
            "score_change": projection["score_change"],
            "previous_default_probability": projection["previous_default_probability"],
            "projected_default_probability": projection["projected_default_probability"],
            "risk_level": projection["risk_level"],
            "transaction_type": payload.transaction_type,
            "status": str(normalized["status"]),
            "expected_amount": float(normalized["expected_amount"]),
            "transaction_date": normalized["transaction_date"],
        }

    def submit_transaction(self, customer_id: int, payload: SimulationTransactionRequest) -> dict:
        self.startup()
        now = datetime.now(UTC).replace(tzinfo=None)
        normalized = self._normalize_payload(customer_id=customer_id, payload=payload, now=now)
        simulation_id = uuid4().hex
        transaction_id = f"SIM::{simulation_id[:12]}"
        raw_json = self._build_raw_json(payload, status=str(normalized["status"]))

        with self.engine.begin() as connection:
            profile_row, score_row, loan_rows, transaction_rows = self._load_customer_context(
                connection=connection,
                customer_id=customer_id,
            )
            projected_transaction = self._build_projected_transaction(
                customer_id=customer_id,
                profile_row=profile_row,
                payload=payload,
                normalized=normalized,
                transaction_id=transaction_id,
            )
            projection, new_features = self._project_score(
                customer_id=customer_id,
                profile_row=profile_row,
                score_row=score_row,
                loan_rows=loan_rows,
                transaction_rows=transaction_rows,
                projected_transaction=projected_transaction,
                as_of=now,
            )

            connection.execute(
                self.bronze_table.insert(),
                {
                    "simulation_id": simulation_id,
                    "customer_id": customer_id,
                    "institution_id": profile_row.get("institution_id"),
                    "source_system": profile_row.get("source_system"),
                    "transaction_type": payload.transaction_type,
                    "transaction_date": normalized["transaction_date"],
                    "amount": payload.amount,
                    "expected_amount": normalized["expected_amount"],
                    "days_past_due": normalized["days_past_due"],
                    "status": normalized["status"],
                    "reference_id": normalized["reference_id"],
                    "raw_json": raw_json,
                    "ingested_at": now,
                },
            )

            connection.execute(
                self.silver_table.insert(),
                {
                    "silver_event_id": f"SILVER::{simulation_id[:16]}",
                    "simulation_id": simulation_id,
                    "customer_id": customer_id,
                    "transaction_id": transaction_id,
                    "transaction_type": payload.transaction_type,
                    "transaction_date": normalized["transaction_date"],
                    "amount": payload.amount,
                    "expected_amount": normalized["expected_amount"],
                    "delta_amount": normalized["delta_amount"],
                    "days_past_due": normalized["days_past_due"],
                    "status": normalized["status"],
                    "reference_id": normalized["reference_id"],
                    "processed_at": now,
                },
            )

            connection.execute(
                text(
                    """
                    INSERT INTO customer_transactions (
                        transaction_id, customer_id, institution_id, source_system, transaction_type, transaction_date, amount,
                        expected_amount, delta_amount, status, reference_id, days_past_due
                    ) VALUES (
                        :transaction_id, :customer_id, :institution_id, :source_system, :transaction_type, :transaction_date, :amount,
                        :expected_amount, :delta_amount, :status, :reference_id, :days_past_due
                    )
                    """
                ),
                {
                    "transaction_id": transaction_id,
                    "customer_id": customer_id,
                    "institution_id": profile_row.get("institution_id"),
                    "source_system": profile_row.get("source_system"),
                    "transaction_type": payload.transaction_type,
                    "transaction_date": normalized["transaction_date"],
                    "amount": payload.amount,
                    "expected_amount": normalized["expected_amount"],
                    "delta_amount": normalized["delta_amount"],
                    "status": normalized["status"],
                    "reference_id": normalized["reference_id"],
                    "days_past_due": normalized["days_past_due"],
                },
            )

            history_count = connection.execute(
                text("SELECT COUNT(*) FROM customer_credit_score_history WHERE customer_id = :customer_id"),
                {"customer_id": customer_id},
            ).scalar_one()
            if int(history_count) == 0:
                connection.execute(
                    self.history_table.insert(),
                    {
                        "customer_id": customer_id,
                        "credit_score": int(score_row["credit_score"]),
                        "default_probability": float(score_row["default_probability"]),
                        "risk_level": str(score_row["risk_level"]),
                        "score_generated_at": _safe_datetime(score_row.get("score_generated_at"), now),
                        "source": "INITIAL",
                    },
                )

            connection.execute(
                text("DELETE FROM customer_credit_features WHERE customer_id = :customer_id"),
                {"customer_id": customer_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO customer_credit_features (
                        customer_id, total_accounts, account_age_days, avg_balance, max_balance, min_balance,
                        total_loans, active_loans, closed_loans, total_payments, late_payments, late_payment_ratio,
                        max_days_past_due, avg_days_past_due, total_debt, debt_to_income_ratio,
                        credit_utilization, account_stability
                    ) VALUES (
                        :customer_id, :total_accounts, :account_age_days, :avg_balance, :max_balance, :min_balance,
                        :total_loans, :active_loans, :closed_loans, :total_payments, :late_payments, :late_payment_ratio,
                        :max_days_past_due, :avg_days_past_due, :total_debt, :debt_to_income_ratio,
                        :credit_utilization, :account_stability
                    )
                    """
                ),
                {column: new_features[column] for column in FEATURE_COLUMNS} | {"customer_id": customer_id},
            )

            connection.execute(
                text("DELETE FROM customer_credit_scores WHERE customer_id = :customer_id"),
                {"customer_id": customer_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO customer_credit_scores (
                        customer_id, institution_id, source_system, credit_score, default_probability, risk_level, score_generated_at
                    ) VALUES (
                        :customer_id, :institution_id, :source_system, :credit_score, :default_probability, :risk_level, :score_generated_at
                    )
                    """
                ),
                {
                    "customer_id": customer_id,
                    "institution_id": new_features["institution_id"],
                    "source_system": new_features["source_system"],
                    "credit_score": projection["new_credit_score"],
                    "default_probability": projection["new_default_probability"],
                    "risk_level": projection["risk_level"],
                    "score_generated_at": now,
                },
            )
            connection.execute(
                self.history_table.insert(),
                {
                    "customer_id": customer_id,
                    "credit_score": projection["new_credit_score"],
                    "default_probability": projection["new_default_probability"],
                    "risk_level": projection["risk_level"],
                    "score_generated_at": now,
                    "source": "SIMULATION",
                },
            )

        return {
            "simulation_id": simulation_id,
            "transaction_id": transaction_id,
            "previous_credit_score": projection["previous_credit_score"],
            "new_credit_score": projection["new_credit_score"],
            "score_change": projection["score_change"],
            "previous_default_probability": projection["previous_default_probability"],
            "new_default_probability": projection["new_default_probability"],
            "risk_level": projection["risk_level"],
            "ingested_at": now,
        }
