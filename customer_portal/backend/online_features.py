from __future__ import annotations

import re
from datetime import date
from statistics import mean, stdev

from pipeline.ml.scoring_common import FEATURE_COLUMNS

_ACTIVE_LOAN_PATTERN = re.compile(r"ACTIVE|APPROVED", re.IGNORECASE)
_CLOSED_LOAN_PATTERN = re.compile(r"CLOSED|COMPLETED", re.IGNORECASE)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _status_matches(value: object, pattern: re.Pattern[str]) -> bool:
    return bool(pattern.search(str(value or "")))


def _mean_or_zero(values: list[float]) -> float:
    return mean(values) if values else 0.0


def build_online_feature_row(
    customer_id: int,
    profile: dict,
    loans: list[dict],
    transactions: list[dict],
    as_of: date | None = None,
) -> dict[str, float | int | str]:
    institution_values = {str(row.get("institution_id")) for row in [profile, *loans, *transactions] if row.get("institution_id")}
    source_values = {str(row.get("source_system")) for row in [profile, *loans, *transactions] if row.get("source_system")}

    payment_rows = [row for row in transactions if str(row.get("transaction_type") or "").upper() == "LOAN_PAYMENT"]
    balance_rows = [row for row in transactions if str(row.get("transaction_type") or "").upper() == "BALANCE_SNAPSHOT"]

    total_accounts = len({str(row.get("reference_id")) for row in balance_rows if row.get("reference_id")})
    balance_dates = [row.get("transaction_date") for row in balance_rows if isinstance(row.get("transaction_date"), date)]
    balances = [_safe_float(row.get("amount"), 0.0) for row in balance_rows]
    credit_limits = [_safe_float(row.get("expected_amount"), 0.0) for row in balance_rows if _safe_float(row.get("expected_amount"), 0.0) > 0]
    balance_dpd = [_safe_int(row.get("days_past_due"), 0) for row in balance_rows]

    total_payments = len({str(row.get("transaction_id")) for row in payment_rows if row.get("transaction_id")})
    late_payments = sum(1 for row in payment_rows if _safe_int(row.get("days_past_due"), 0) > 0)

    loan_dpd = [_safe_int(row.get("days_past_due"), 0) for row in loans]
    active_loans = sum(1 for row in loans if _status_matches(row.get("loan_status"), _ACTIVE_LOAN_PATTERN))
    closed_loans = sum(1 for row in loans if _status_matches(row.get("loan_status"), _CLOSED_LOAN_PATTERN))
    total_loans = len({str(row.get("loan_id")) for row in loans if row.get("loan_id")})

    active_loan_debt = sum(
        _safe_float(row.get("loan_amount"), 0.0)
        for row in loans
        if _status_matches(row.get("loan_status"), _ACTIVE_LOAN_PATTERN)
    )
    positive_balance_sum = sum(max(value, 0.0) for value in balances)
    income = _safe_float(profile.get("income"), 0.0)

    anchor_date = as_of or max(balance_dates, default=None) or date.today()
    account_age_days = 0
    if balance_dates:
        account_age_days = max(0, (anchor_date - min(balance_dates)).days)

    avg_balance = _mean_or_zero(balances)
    avg_credit_limit = _mean_or_zero(credit_limits)
    avg_loan_dpd = _mean_or_zero([float(value) for value in loan_dpd])
    avg_balance_dpd = _mean_or_zero([float(value) for value in balance_dpd])

    if loan_dpd and balance_dpd:
        avg_days_past_due = (avg_loan_dpd + avg_balance_dpd) / 2.0
    elif loan_dpd:
        avg_days_past_due = avg_loan_dpd
    else:
        avg_days_past_due = avg_balance_dpd

    institution_id = (
        "multi"
        if len(institution_values) > 1
        else next(iter(institution_values), str(profile.get("institution_id") or "unknown"))
    )
    source_system = (
        "multi"
        if len(source_values) > 1
        else next(iter(source_values), str(profile.get("source_system") or "unknown"))
    )

    feature_row = {
        "customer_id": int(customer_id),
        "institution_id": institution_id,
        "source_system": source_system,
        "total_accounts": int(total_accounts),
        "account_age_days": int(account_age_days),
        "avg_balance": float(avg_balance),
        "max_balance": float(max(balances) if balances else 0.0),
        "min_balance": float(min(balances) if balances else 0.0),
        "total_loans": int(total_loans),
        "active_loans": int(active_loans),
        "closed_loans": int(closed_loans),
        "total_payments": int(total_payments),
        "late_payments": int(late_payments),
        "late_payment_ratio": float((late_payments / total_payments) if total_payments > 0 else 0.0),
        "max_days_past_due": int(max(loan_dpd + balance_dpd) if (loan_dpd or balance_dpd) else 0),
        "avg_days_past_due": float(avg_days_past_due),
        "total_debt": float(active_loan_debt + positive_balance_sum),
        "debt_to_income_ratio": float(((active_loan_debt + positive_balance_sum) / income) if income > 0 else 0.0),
        "credit_utilization": float((avg_balance / avg_credit_limit) if avg_credit_limit > 0 else 0.0),
        "account_stability": float(stdev(balances) if len(balances) >= 2 else 0.0),
    }
    return feature_row


def feature_values_only(feature_row: dict[str, float | int | str]) -> dict[str, float | int]:
    return {column: feature_row[column] for column in FEATURE_COLUMNS}
