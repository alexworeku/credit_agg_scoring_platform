from __future__ import annotations

from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def explain_credit_score(
    features: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    debt_to_income = _to_float(features.get("debt_to_income_ratio"))
    late_ratio = _to_float(features.get("late_payment_ratio"))
    utilization = _to_float(features.get("credit_utilization"))
    max_dpd = _to_int(features.get("max_days_past_due"))
    account_age_days = _to_int(features.get("account_age_days"))
    total_accounts = _to_int(features.get("total_accounts"))
    active_loans = _to_int(features.get("active_loans"))

    factors: list[dict[str, Any]] = []

    def add_factor(
        name: str,
        impact: str,
        value: float | int | str | None,
        benchmark: str,
        detail: str,
        priority_score: float,
    ) -> None:
        factors.append(
            {
                "factor": name,
                "impact": impact,
                "value": value,
                "benchmark": benchmark,
                "detail": detail,
                "_priority_score": priority_score,
            }
        )

    if debt_to_income > 0.40:
        add_factor(
            "Debt-to-income ratio",
            "negative",
            round(debt_to_income, 2),
            "<= 0.35",
            "Outstanding debt is high relative to income, which increases repayment risk.",
            debt_to_income - 0.40,
        )
    elif debt_to_income <= 0.25:
        add_factor(
            "Debt-to-income ratio",
            "positive",
            round(debt_to_income, 2),
            "<= 0.35",
            "Debt load is well managed relative to income.",
            0.35 - debt_to_income,
        )

    if late_ratio > 0.08:
        add_factor(
            "Late payment ratio",
            "negative",
            round(late_ratio, 2),
            "<= 0.05",
            "Recent payment behavior shows frequent late payments.",
            late_ratio - 0.08,
        )
    elif late_ratio <= 0.02:
        add_factor(
            "Late payment ratio",
            "positive",
            round(late_ratio, 2),
            "<= 0.05",
            "Payment history is consistently on time.",
            0.05 - late_ratio,
        )

    if utilization > 0.65:
        add_factor(
            "Credit utilization",
            "negative",
            round(utilization, 2),
            "<= 0.30",
            "A high share of available credit is currently in use.",
            utilization - 0.65,
        )
    elif 0 <= utilization <= 0.30:
        add_factor(
            "Credit utilization",
            "positive",
            round(utilization, 2),
            "<= 0.30",
            "Utilization is in a healthy range.",
            0.30 - utilization,
        )

    if max_dpd > 30:
        add_factor(
            "Maximum days past due",
            "negative",
            max_dpd,
            "<= 7 days",
            "There are severe delinquency events in the account history.",
            float(max_dpd - 30),
        )
    elif max_dpd == 0:
        add_factor(
            "Maximum days past due",
            "positive",
            max_dpd,
            "<= 7 days",
            "No delinquency spikes were observed.",
            7.0,
        )

    if account_age_days < 365:
        add_factor(
            "Account age",
            "negative",
            account_age_days,
            ">= 365 days",
            "Short credit history reduces confidence in long-term behavior.",
            float(365 - account_age_days) / 365.0,
        )
    elif account_age_days >= 365 * 3:
        add_factor(
            "Account age",
            "positive",
            account_age_days,
            ">= 365 days",
            "Long account history supports score stability.",
            float(account_age_days) / 365.0,
        )

    if active_loans > 5:
        add_factor(
            "Active loans",
            "negative",
            active_loans,
            "<= 5",
            "Many concurrent active loans can increase repayment pressure.",
            float(active_loans - 5),
        )

    if total_accounts >= 4 and account_age_days >= 365:
        add_factor(
            "Account depth",
            "positive",
            total_accounts,
            ">= 3 accounts",
            "A broader credit profile can improve risk visibility.",
            float(total_accounts - 3),
        )

    factors_sorted = sorted(
        factors,
        key=lambda factor: (
            0 if factor["impact"] == "negative" else 1,
            -float(factor["_priority_score"]),
        ),
    )[:6]
    for factor in factors_sorted:
        factor.pop("_priority_score", None)

    recommendations: list[dict[str, str]] = []
    if debt_to_income > 0.40:
        recommendations.append(
            {
                "title": "Lower debt-to-income ratio",
                "priority": "high",
                "why_it_matters": "High debt pressure is one of the strongest downward drivers.",
                "action": "Prioritize paying down the highest-interest balances and avoid new debt for 60-90 days.",
            }
        )
    if late_ratio > 0.05 or max_dpd > 7:
        recommendations.append(
            {
                "title": "Improve payment consistency",
                "priority": "high",
                "why_it_matters": "Late payments quickly increase perceived default risk.",
                "action": "Enable autopay for minimums and schedule a weekly payment review reminder.",
            }
        )
    if utilization > 0.50:
        recommendations.append(
            {
                "title": "Reduce credit utilization",
                "priority": "medium",
                "why_it_matters": "Utilization above healthy bands can suppress the score.",
                "action": "Target utilization below 30% by paying revolving balances before statement close.",
            }
        )
    if active_loans > 5:
        recommendations.append(
            {
                "title": "Consolidate active obligations",
                "priority": "medium",
                "why_it_matters": "Multiple active obligations raise repayment complexity.",
                "action": "Evaluate refinancing or consolidation options to reduce monthly payment count.",
            }
        )
    if account_age_days < 365:
        recommendations.append(
            {
                "title": "Build account tenure",
                "priority": "low",
                "why_it_matters": "Longer account history helps stabilize risk assessment.",
                "action": "Keep older accounts in good standing and avoid closing long-tenure lines.",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "title": "Maintain strong profile",
                "priority": "low",
                "why_it_matters": "Current behavior is broadly aligned with healthy score drivers.",
                "action": "Continue on-time payments and keep utilization in the 10-30% range.",
            }
        )

    return factors_sorted, recommendations


def build_personalized_plan(
    features: dict[str, Any],
    score_history: list[dict[str, Any]],
    contributors: list[dict[str, Any]],
    base_recommendations: list[dict[str, str]],
) -> list[dict[str, str]]:
    debt_to_income = _to_float(features.get("debt_to_income_ratio"))
    late_ratio = _to_float(features.get("late_payment_ratio"))
    utilization = _to_float(features.get("credit_utilization"))

    plan: list[dict[str, str]] = []

    if len(score_history) >= 2:
        first = _to_int(score_history[0].get("credit_score"), default=0)
        latest = _to_int(score_history[-1].get("credit_score"), default=0)
        drop = latest - first
        if drop <= -25:
            plan.append(
                {
                    "title": "Stop score decline first",
                    "priority": "high",
                    "why_it_matters": "Your score trend has declined materially over recent periods.",
                    "action": "For the next 30 days avoid new borrowing and focus only on current obligations.",
                }
            )
        elif drop >= 20:
            plan.append(
                {
                    "title": "Protect the positive momentum",
                    "priority": "low",
                    "why_it_matters": "Your recent trend is improving.",
                    "action": "Keep utilization stable and continue on-time payments for at least two more cycles.",
                }
            )

    negative_contributors = [
        item for item in contributors if str(item.get("contribution", "")).lower() == "negative"
    ]
    if len(negative_contributors) >= 3:
        plan.append(
            {
                "title": "Address recurring negative transactions",
                "priority": "high",
                "why_it_matters": "Multiple recent transactions show late or past-due behavior.",
                "action": "Build a payment calendar and clear the most overdue items first within 14 days.",
            }
        )

    if debt_to_income > 0.40:
        plan.append(
            {
                "title": "Reduce debt burden over 60 days",
                "priority": "high",
                "why_it_matters": "Debt relative to income remains a major risk signal.",
                "action": "Target paying at least 10-15% of high-interest balances this cycle.",
            }
        )

    if utilization > 0.50:
        plan.append(
            {
                "title": "Lower revolving utilization",
                "priority": "medium",
                "why_it_matters": "High utilization suppresses score recovery.",
                "action": "Keep statement balances below 30% of limit, starting with the largest account.",
            }
        )

    if late_ratio > 0.05:
        plan.append(
            {
                "title": "Eliminate late payments in next 90 days",
                "priority": "high",
                "why_it_matters": "Payment timeliness is a high-weight score driver.",
                "action": "Set autopay minimums and keep a weekly reminder to confirm account coverage.",
            }
        )

    merged = plan + base_recommendations
    deduped: list[dict[str, str]] = []
    seen_titles: set[str] = set()
    for rec in merged:
        title = rec.get("title", "").strip().lower()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        deduped.append(rec)

    return deduped[:6]
