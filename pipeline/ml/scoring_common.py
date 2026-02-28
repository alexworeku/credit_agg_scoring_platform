from __future__ import annotations

import json
import math
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np

FEATURE_COLUMNS = [
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

_EPSILON = 1e-6


def clamp_probability(value: float) -> float:
    return max(_EPSILON, min(1.0 - _EPSILON, float(value)))


def clamp_score(value: float, min_score: int = 300, max_score: int = 850) -> int:
    return int(max(min_score, min(max_score, round(float(value)))))


def _logit(probability: float) -> float:
    clipped = clamp_probability(probability)
    return math.log(clipped / (1.0 - clipped))


def _sigmoid(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-value)
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


def calibrate_probability(raw_probability: float, metadata: dict[str, Any]) -> float:
    calibration = metadata.get("probability_calibration") or {}
    method = calibration.get("method")
    if method != "platt_logit":
        return clamp_probability(raw_probability)

    intercept = float(calibration.get("intercept", 0.0))
    coef = float(calibration.get("coef", 1.0))
    calibrated = _sigmoid(intercept + (coef * _logit(raw_probability)))
    return clamp_probability(calibrated)


def probability_to_score(default_probability: float, metadata: dict[str, Any]) -> int:
    scorecard = metadata.get("scorecard") or {}
    method = scorecard.get("method")
    min_score = int(scorecard.get("min_score", 300))
    max_score = int(scorecard.get("max_score", 850))

    if method == "log_odds":
        probability = clamp_probability(default_probability)
        pdo = float(scorecard.get("pdo", 20.0))
        base_score = float(scorecard.get("base_score", 600.0))
        base_odds = max(float(scorecard.get("base_odds", 20.0)), _EPSILON)
        factor = pdo / math.log(2.0)
        offset = base_score - (factor * math.log(base_odds))
        score = offset + (factor * math.log((1.0 - probability) / probability))
        return clamp_score(score, min_score=min_score, max_score=max_score)

    linear_score = 850.0 - (clamp_probability(default_probability) * 550.0)
    return clamp_score(linear_score, min_score=min_score, max_score=max_score)


def map_risk_level(default_probability: float, metadata: dict[str, Any]) -> str:
    risk_bands = metadata.get("risk_bands") or {}
    low_max = float(risk_bands.get("low_max_default_probability", 0.20))
    medium_max = float(risk_bands.get("medium_max_default_probability", 0.50))

    if default_probability <= low_max:
        return "LOW"
    if default_probability <= medium_max:
        return "MEDIUM"
    return "HIGH"


@dataclass
class ModelScorer:
    model: Any
    metadata: dict[str, Any]

    @property
    def feature_columns(self) -> list[str]:
        return list(self.metadata.get("feature_columns") or FEATURE_COLUMNS)

    def _build_matrix(self, rows: Sequence[dict[str, Any]]) -> np.ndarray:
        return np.array(
            [[float(row.get(column) or 0.0) for column in self.feature_columns] for row in rows],
            dtype=np.float64,
        )

    def score_rows(self, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []

        matrix = self._build_matrix(rows)
        raw_probabilities = self.model.predict_proba(matrix)[:, 1]
        scored_rows: list[dict[str, Any]] = []
        for row, raw_probability in zip(rows, raw_probabilities, strict=True):
            calibrated_probability = calibrate_probability(float(raw_probability), self.metadata)
            scored_rows.append(
                {
                    "raw_default_probability": float(raw_probability),
                    "default_probability": calibrated_probability,
                    "credit_score": probability_to_score(calibrated_probability, self.metadata),
                    "risk_level": map_risk_level(calibrated_probability, self.metadata),
                    "features": {column: row.get(column) for column in self.feature_columns},
                }
            )
        return scored_rows

    def score_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return self.score_rows([row])[0]

    @classmethod
    def load(cls, model_path: str | Path, metadata_path: str | Path) -> "ModelScorer":
        model_file = Path(model_path)
        metadata_file = Path(metadata_path)
        with model_file.open("rb") as handle:
            model = pickle.load(handle)
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        return cls(model=model, metadata=metadata)
