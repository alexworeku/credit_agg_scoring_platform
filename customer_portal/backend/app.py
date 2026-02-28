from __future__ import annotations

import os
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from customer_portal.backend.auth import (
    AuthUser,
    create_user,
    decode_access_token,
    get_user_by_id,
    issue_access_token,
    init_auth_db,
    list_users,
    verify_user_credentials,
)
from customer_portal.backend.ai_insights import AiInsightsService
from customer_portal.backend.explainability import build_personalized_plan, explain_credit_score
from customer_portal.backend.models import (
    AiImprovementAdvisor,
    AiInsightsBundle,
    AiScoreExplanation,
    AuthMeResponse,
    CustomerDetail,
    CustomerProfile,
    CreditScoreSnapshot,
    LoginRequest,
    LoginResponse,
    Recommendation,
    ScoreContributor,
    ScoreFactor,
    ScoreHistoryPoint,
    SimulationStatus,
    SimulationTransactionRequest,
    SimulationTransactionResponse,
    WhatIfScoreResponse,
    TransactionRecord,
    TransactionSummary,
)
from customer_portal.backend.repository import BaseRepository, ParquetRepository, RepositoryError, SqlRepository
from customer_portal.backend.simulation_pipeline import RealtimeSimulationService


def _build_repository() -> BaseRepository:
    database_url = os.getenv("OPERATIONAL_DB_URL")
    if database_url:
        return SqlRepository(database_url=database_url)
    return ParquetRepository(
        customers_path=os.getenv("PORTAL_CUSTOMERS_PATH", "data/medallion/silver/customers"),
        transactions_path=os.getenv("PORTAL_TRANSACTIONS_PATH", "data/medallion/silver/transactions"),
        features_path=os.getenv("PORTAL_FEATURES_PATH", "data/medallion/gold/customer_credit_features"),
        scores_path=os.getenv("PORTAL_SCORES_PATH", "data/medallion/gold/customer_credit_scores"),
    )


def _build_transaction_summary(transactions: list[dict[str, Any]]) -> TransactionSummary:
    latest_transaction_date = transactions[0].get("transaction_date") if transactions else None
    total_payment_amount = 0.0
    late_or_past_due_events = 0
    for transaction in transactions:
        if transaction.get("transaction_type") == "LOAN_PAYMENT":
            total_payment_amount += float(transaction.get("amount") or 0.0)
        if str(transaction.get("status") or "").upper() in {"LATE", "PAST_DUE"}:
            late_or_past_due_events += 1
    return TransactionSummary(
        total_transactions=len(transactions),
        latest_transaction_date=latest_transaction_date,
        total_payment_amount=round(total_payment_amount, 2),
        late_or_past_due_events=late_or_past_due_events,
    )


def _build_score_history(
    score_history_rows: list[dict[str, Any]],
    current_score: dict[str, Any],
    max_points: int = 12,
) -> list[dict[str, Any]]:
    def _normalize_timestamp(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        return None

    if score_history_rows:
        rows = sorted(score_history_rows, key=lambda item: item.get("score_generated_at") or date.min)
        rows = rows[-max_points:]
        return [
            {
                "period": str(item.get("score_generated_at") or "")[:7]
                or str(current_score.get("score_generated_at") or date.today())[:7],
                "credit_score": int(item.get("credit_score") or 0),
                "default_probability": float(item.get("default_probability") or 0.0),
                "is_estimated": False,
                "score_generated_at": _normalize_timestamp(item.get("score_generated_at")),
            }
            for item in rows
        ]

    return [
        {
            "period": str(current_score.get("score_generated_at") or date.today())[:7],
            "credit_score": int(current_score["credit_score"]),
            "default_probability": float(current_score["default_probability"]),
            "is_estimated": False,
            "score_generated_at": _normalize_timestamp(current_score.get("score_generated_at")),
        }
    ]


def _build_score_contributors(
    transactions: list[dict[str, Any]],
    limit: int = 20,
) -> list[dict[str, Any]]:
    contributors: list[dict[str, Any]] = []
    for transaction in transactions:
        status = str(transaction.get("status") or "").upper()
        days_past_due = int(transaction.get("days_past_due") or 0)
        transaction_date = transaction.get("transaction_date")

        if status in {"LATE", "PAST_DUE"}:
            contribution = "negative"
            source_impact = -days_past_due
            reason = f"Status {status}; {days_past_due} days past due."
        elif status in {"ON_TIME", "CURRENT"}:
            contribution = "positive"
            source_impact = 0
            reason = f"Status {status}; no past-due days reported."
        else:
            contribution = "neutral"
            source_impact = 0
            reason = f"Status {status or 'UNKNOWN'}."

        contributors.append(
            {
                "transaction_id": str(transaction.get("transaction_id")),
                "transaction_date": transaction_date,
                "transaction_type": str(transaction.get("transaction_type") or "UNKNOWN"),
                "status": status,
                "contribution": contribution,
                "estimated_score_impact": int(source_impact),
                "reason": reason,
            }
        )

    def _priority(item: dict[str, Any]) -> int:
        if item["contribution"] == "negative":
            return 0
        if item["contribution"] == "positive":
            return 1
        return 2

    contributors.sort(
        key=lambda item: (
            _priority(item),
            -abs(int(item.get("estimated_score_impact") or 0)),
            -(item.get("transaction_date").toordinal() if isinstance(item.get("transaction_date"), date) else 0),
        ),
    )
    return contributors[:limit]


def _compose_customer_dashboard(customer_id: int, repository: BaseRepository) -> tuple[CustomerDetail, list[dict], list[dict]]:
    profile_data = repository.get_profile(customer_id)
    score_data = repository.get_score(customer_id)
    features_data = repository.get_features(customer_id)
    if profile_data is None or score_data is None or features_data is None:
        raise HTTPException(status_code=404, detail=f"customer_id {customer_id} not found")

    transactions = repository.get_transactions(customer_id=customer_id, limit=500)
    contributors = _build_score_contributors(transactions)
    score_history_rows = repository.get_score_history(customer_id=customer_id, limit=12)
    score_history = _build_score_history(score_history_rows=score_history_rows, current_score=score_data)

    factors, base_recommendations = explain_credit_score(features_data)
    recommendations = build_personalized_plan(
        features=features_data,
        score_history=score_history,
        contributors=contributors,
        base_recommendations=base_recommendations,
    )
    summary = _build_transaction_summary(transactions)

    detail = CustomerDetail(
        profile=CustomerProfile(**profile_data),
        score=CreditScoreSnapshot(**score_data),
        factors=[ScoreFactor(**factor) for factor in factors],
        recommendations=[Recommendation(**recommendation) for recommendation in recommendations],
        metrics=features_data,
        transaction_summary=summary,
    )
    return detail, score_history, contributors


app = FastAPI(
    title="Customer Credit Portal API",
    version="2.0.0",
    description="Customer self-service portal APIs with login, score explanations, score history, and transaction insights.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

repository = _build_repository()
ai_service = AiInsightsService()
simulation_service = (
    RealtimeSimulationService(repository.database_url)
    if isinstance(repository, SqlRepository)
    else None
)
security = HTTPBearer(auto_error=False)
portal_root = Path(__file__).resolve().parents[1]
frontend_dist_dir = portal_root / "frontend_dist"
frontend_assets_dir = frontend_dist_dir / "assets"
if frontend_assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(frontend_assets_dir)), name="assets")


def _frontend_index_path() -> Path:
    return frontend_dist_dir / "index.html"


@app.on_event("startup")
def on_startup() -> None:
    repository.startup()
    if simulation_service is not None:
        simulation_service.startup()
    init_auth_db()

    seed_demo = os.getenv("PORTAL_SEED_DEMO_USER", "true").lower() == "true"
    if not seed_demo:
        return
    existing = list_users()
    if existing:
        return
    candidates = repository.search_customers(query=None, limit=200)
    selected_customer_id = None
    for candidate in candidates:
        customer_id = int(candidate["customer_id"])
        transactions = repository.get_transactions(customer_id=customer_id, limit=5)
        if transactions:
            selected_customer_id = customer_id
            break
    if selected_customer_id is None and candidates:
        selected_customer_id = int(candidates[0]["customer_id"])
    if selected_customer_id is None:
        return
    create_user(
        username="demo",
        password="demo123",
        customer_id=selected_customer_id,
        full_name="Demo Customer",
    )


def _require_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> AuthUser:
    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        payload = decode_access_token(credentials.credentials)
        user = get_user_by_id(int(payload["uid"]))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="User is inactive or not found")
    return user


@app.get("/", include_in_schema=False)
def frontend_index() -> FileResponse:
    index_path = _frontend_index_path()
    if not index_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Frontend build is missing. Run `npm --prefix customer_portal/frontend_react run build`.",
        )
    return FileResponse(index_path)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", **repository.health()}


@app.post("/api/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    user = verify_user_credentials(username=payload.username, password=payload.password)
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    try:
        if (
            repository.get_profile(user.customer_id) is None
            or repository.get_score(user.customer_id) is None
            or repository.get_features(user.customer_id) is None
        ):
            raise HTTPException(status_code=403, detail="Mapped customer profile is not available in portal data")
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    expires_seconds = int(os.getenv("PORTAL_AUTH_TOKEN_TTL_SECONDS", str(8 * 3600)))
    token = issue_access_token(user=user, expiry_minutes=max(1, expires_seconds // 60))
    return LoginResponse(
        access_token=token,
        expires_in_seconds=expires_seconds,
    )


@app.get("/api/auth/me", response_model=AuthMeResponse)
def auth_me(current_user: AuthUser = Depends(_require_current_user)) -> AuthMeResponse:
    return AuthMeResponse(
        user_id=current_user.user_id,
        username=current_user.username,
        full_name=current_user.full_name,
        customer_id=current_user.customer_id,
    )


@app.get("/api/me/dashboard", response_model=CustomerDetail)
def me_dashboard(current_user: AuthUser = Depends(_require_current_user)) -> CustomerDetail:
    try:
        detail, _, _ = _compose_customer_dashboard(customer_id=current_user.customer_id, repository=repository)
        return detail
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/transactions", response_model=list[TransactionRecord])
def me_transactions(
    limit: int = Query(default=100, ge=1, le=500),
    transaction_type: str | None = Query(
        default=None,
        description="Filter by transaction type: LOAN_PAYMENT or BALANCE_SNAPSHOT",
    ),
    current_user: AuthUser = Depends(_require_current_user),
) -> list[TransactionRecord]:
    try:
        rows = repository.get_transactions(
            customer_id=current_user.customer_id,
            limit=limit,
            transaction_type=transaction_type,
        )
        return [TransactionRecord(**row) for row in rows]
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/score-history", response_model=list[ScoreHistoryPoint])
def me_score_history(
    current_user: AuthUser = Depends(_require_current_user),
) -> list[ScoreHistoryPoint]:
    try:
        _, history, _ = _compose_customer_dashboard(customer_id=current_user.customer_id, repository=repository)
        return [ScoreHistoryPoint(**point) for point in history]
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/score-contributors", response_model=list[ScoreContributor])
def me_score_contributors(
    limit: int = Query(default=20, ge=1, le=100),
    current_user: AuthUser = Depends(_require_current_user),
) -> list[ScoreContributor]:
    try:
        transactions = repository.get_transactions(
            customer_id=current_user.customer_id,
            limit=500,
        )
        contributors = _build_score_contributors(transactions=transactions, limit=limit)
        return [ScoreContributor(**item) for item in contributors]
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/credit-plan", response_model=list[Recommendation])
def me_credit_plan(current_user: AuthUser = Depends(_require_current_user)) -> list[Recommendation]:
    try:
        detail, _, _ = _compose_customer_dashboard(
            customer_id=current_user.customer_id,
            repository=repository,
        )
        return detail.recommendations
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _resolve_ai_insights_for_current_user(current_user: AuthUser) -> dict[str, Any]:
    detail, history, contributors = _compose_customer_dashboard(
        customer_id=current_user.customer_id,
        repository=repository,
    )
    return ai_service.build_insights(
        customer_id=current_user.customer_id,
        score=detail.score.model_dump(),
        metrics=detail.metrics,
        history=history,
        contributors=contributors,
    )


@app.get("/api/me/ai/score-explanation", response_model=AiScoreExplanation)
def me_ai_score_explanation(current_user: AuthUser = Depends(_require_current_user)) -> AiScoreExplanation:
    try:
        bundle = _resolve_ai_insights_for_current_user(current_user=current_user)
        return AiScoreExplanation(**bundle["score_explanation"])
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/ai/improvement-advisor", response_model=AiImprovementAdvisor)
def me_ai_improvement_advisor(current_user: AuthUser = Depends(_require_current_user)) -> AiImprovementAdvisor:
    try:
        bundle = _resolve_ai_insights_for_current_user(current_user=current_user)
        return AiImprovementAdvisor(**bundle["improvement_advisor"])
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/ai/insights", response_model=AiInsightsBundle)
def me_ai_insights(current_user: AuthUser = Depends(_require_current_user)) -> AiInsightsBundle:
    try:
        bundle = _resolve_ai_insights_for_current_user(current_user=current_user)
        return AiInsightsBundle(**bundle)
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/me/simulator/status", response_model=SimulationStatus)
def me_simulator_status(current_user: AuthUser = Depends(_require_current_user)) -> SimulationStatus:
    _ = current_user
    if simulation_service is None:
        return SimulationStatus(
            enabled=False,
            mode="parquet",
            reason="Simulator requires OPERATIONAL_DB_URL (SQL operational database mode).",
        )
    return SimulationStatus(enabled=True, mode="operational_db", reason=None)


@app.post("/api/me/simulator/transaction", response_model=SimulationTransactionResponse)
def me_simulate_transaction(
    payload: SimulationTransactionRequest,
    current_user: AuthUser = Depends(_require_current_user),
) -> SimulationTransactionResponse:
    if simulation_service is None:
        raise HTTPException(
            status_code=400,
            detail="Simulation endpoint requires OPERATIONAL_DB_URL with SQL operational database mode.",
        )
    try:
        result = simulation_service.submit_transaction(
            customer_id=current_user.customer_id,
            payload=payload,
        )
        return SimulationTransactionResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Simulation failed: {exc}") from exc


@app.post("/api/me/what-if-score", response_model=WhatIfScoreResponse)
def me_what_if_score(
    payload: SimulationTransactionRequest,
    current_user: AuthUser = Depends(_require_current_user),
) -> WhatIfScoreResponse:
    if simulation_service is None:
        raise HTTPException(
            status_code=400,
            detail="What-if scoring requires OPERATIONAL_DB_URL with SQL operational database mode.",
        )
    try:
        result = simulation_service.preview_transaction(
            customer_id=current_user.customer_id,
            payload=payload,
        )
        return WhatIfScoreResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"What-if scoring failed: {exc}") from exc


@app.get("/{full_path:path}", include_in_schema=False)
def frontend_spa(full_path: str) -> FileResponse:
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API endpoint not found")
    if full_path in {"health", "openapi.json", "docs", "redoc"}:
        raise HTTPException(status_code=404, detail="Not found")

    if frontend_dist_dir.exists():
        requested = frontend_dist_dir / full_path
        if requested.is_file():
            return FileResponse(requested)
        if full_path.startswith("assets/"):
            raise HTTPException(status_code=404, detail="Asset not found")

    index_path = _frontend_index_path()
    if not index_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Frontend build is missing. Run `npm --prefix customer_portal/frontend_react run build`.",
        )
    return FileResponse(index_path)
