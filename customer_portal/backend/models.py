from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel


class CustomerProfile(BaseModel):
    customer_id: int
    institution_id: str | None = None
    source_system: str | None = None
    gender: str | None = None
    age: int | None = None
    income: float | None = None
    employment_type: str | None = None
    education: str | None = None
    housing_type: str | None = None
    account_open_date: date | None = None


class CreditScoreSnapshot(BaseModel):
    credit_score: int
    risk_level: str
    default_probability: float
    score_generated_at: datetime | None = None


class ScoreFactor(BaseModel):
    factor: str
    impact: Literal["positive", "negative", "neutral"]
    value: float | int | str | None = None
    benchmark: str
    detail: str


class Recommendation(BaseModel):
    title: str
    priority: Literal["high", "medium", "low"]
    why_it_matters: str
    action: str


class TransactionRecord(BaseModel):
    transaction_id: str
    customer_id: int
    transaction_type: Literal["LOAN_PAYMENT", "BALANCE_SNAPSHOT"]
    transaction_date: date | None = None
    amount: float | None = None
    expected_amount: float | None = None
    delta_amount: float | None = None
    status: str
    reference_id: str | None = None
    days_past_due: int | None = None


class TransactionSummary(BaseModel):
    total_transactions: int
    latest_transaction_date: date | None = None
    total_payment_amount: float
    late_or_past_due_events: int


class CustomerDetail(BaseModel):
    profile: CustomerProfile
    score: CreditScoreSnapshot
    factors: list[ScoreFactor]
    recommendations: list[Recommendation]
    metrics: dict[str, float | int | str | None]
    transaction_summary: TransactionSummary


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int


class AuthMeResponse(BaseModel):
    user_id: int
    username: str
    full_name: str | None = None
    customer_id: int


class ScoreHistoryPoint(BaseModel):
    period: str
    credit_score: int
    default_probability: float
    is_estimated: bool
    score_generated_at: datetime | None = None


class ScoreContributor(BaseModel):
    transaction_id: str
    transaction_date: date | None = None
    transaction_type: str
    status: str
    contribution: Literal["positive", "negative", "neutral"]
    estimated_score_impact: int
    reason: str


class AiScoreExplanation(BaseModel):
    headline: str
    summary: str
    strengths: list[str]
    risk_factors: list[str]
    recommendation: str
    confidence_note: str


class AiAdvisorAction(BaseModel):
    action: str
    impact: Literal["high", "medium", "low"]
    expected_outcome: str
    timeline: str


class AiImprovementAdvisor(BaseModel):
    overview: str
    prioritized_actions: list[AiAdvisorAction]
    momentum_message: str


class AiInsightsBundle(BaseModel):
    score_explanation: AiScoreExplanation
    improvement_advisor: AiImprovementAdvisor


class SimulationStatus(BaseModel):
    enabled: bool
    mode: str
    reason: str | None = None


class SimulationTransactionRequest(BaseModel):
    transaction_type: Literal["LOAN_PAYMENT", "BALANCE_SNAPSHOT"]
    amount: float
    expected_amount: float | None = None
    days_past_due: int = 0
    status: str | None = None
    reference_id: str | None = None
    transaction_date: date | None = None
    raw_attributes: dict[str, Any] | None = None


class SimulationTransactionResponse(BaseModel):
    simulation_id: str
    transaction_id: str
    previous_credit_score: int
    new_credit_score: int
    score_change: int
    previous_default_probability: float
    new_default_probability: float
    risk_level: str
    ingested_at: datetime


class WhatIfScoreResponse(BaseModel):
    previous_credit_score: int
    projected_credit_score: int
    score_change: int
    previous_default_probability: float
    projected_default_probability: float
    risk_level: str
    transaction_type: Literal["LOAN_PAYMENT", "BALANCE_SNAPSHOT"]
    status: str
    expected_amount: float
    transaction_date: date
