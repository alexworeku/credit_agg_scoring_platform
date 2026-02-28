from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class CachedItem:
    created_at: float
    value: dict[str, Any]


class AiInsightsService:
    def __init__(self) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.timeout_seconds = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))
        self.cache_ttl_seconds = int(os.getenv("AI_INSIGHTS_CACHE_TTL_SECONDS", str(60 * 30)))
        self._cache: dict[str, CachedItem] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _cache_get(self, key: str) -> dict[str, Any] | None:
        item = self._cache.get(key)
        if item is None:
            return None
        if (time.time() - item.created_at) > self.cache_ttl_seconds:
            self._cache.pop(key, None)
            return None
        return item.value

    def _cache_set(self, key: str, value: dict[str, Any]) -> None:
        self._cache[key] = CachedItem(created_at=time.time(), value=value)

    def _call_openai_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        payload = {
            "model": self.model,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url="https://api.openai.com/v1/chat/completions",
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
            content = body["choices"][0]["message"]["content"]
            return json.loads(content)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, KeyError, json.JSONDecodeError):
            return None

    @staticmethod
    def _score_band(score: int) -> str:
        if score >= 760:
            return "EXCELLENT"
        if score >= 700:
            return "GOOD"
        if score >= 640:
            return "FAIR"
        return "REBUILDING"

    @staticmethod
    def _feature_snapshot(metrics: dict[str, Any], score: dict[str, Any]) -> dict[str, Any]:
        return {
            "credit_score": int(score["credit_score"]),
            "risk_level": str(score["risk_level"]),
            "default_probability": float(score["default_probability"]),
            "late_payment_ratio": float(metrics.get("late_payment_ratio") or 0.0),
            "debt_to_income_ratio": float(metrics.get("debt_to_income_ratio") or 0.0),
            "credit_utilization": float(metrics.get("credit_utilization") or 0.0),
            "account_age_days": int(metrics.get("account_age_days") or 0),
            "total_payments": int(metrics.get("total_payments") or 0),
            "late_payments": int(metrics.get("late_payments") or 0),
            "max_days_past_due": int(metrics.get("max_days_past_due") or 0),
            "account_stability": float(metrics.get("account_stability") or 0.0),
            "total_debt": float(metrics.get("total_debt") or 0.0),
        }

    @staticmethod
    def _contains_number(text: str) -> bool:
        return any(char.isdigit() for char in text)

    @staticmethod
    def _numeric_context_for_item(item: str, feature_snapshot: dict[str, Any]) -> str:
        lower = item.lower()
        late_payments = int(feature_snapshot.get("late_payments") or 0)
        total_payments = int(feature_snapshot.get("total_payments") or 0)
        late_ratio = float(feature_snapshot.get("late_payment_ratio") or 0.0)
        debt_to_income_ratio = float(feature_snapshot.get("debt_to_income_ratio") or 0.0)
        credit_utilization = float(feature_snapshot.get("credit_utilization") or 0.0)
        account_age_days = int(feature_snapshot.get("account_age_days") or 0)
        max_days_past_due = int(feature_snapshot.get("max_days_past_due") or 0)

        if "late" in lower or "payment" in lower:
            return f"late {late_payments}/{total_payments} ({late_ratio * 100:.1f}%)"
        if "debt" in lower or "income" in lower:
            return f"DTI {debt_to_income_ratio * 100:.1f}%"
        if "utilization" in lower or "credit" in lower or "balance" in lower:
            return f"utilization {credit_utilization * 100:.1f}%"
        if "history" in lower or "age" in lower:
            return f"account age {account_age_days / 365:.1f} years"
        if "past due" in lower or "delinquency" in lower:
            return f"max past due {max_days_past_due} days"
        return ""

    def _ensure_numeric_context(self, items: list[str], feature_snapshot: dict[str, Any]) -> list[str]:
        result: list[str] = []
        for item in items:
            cleaned = str(item).strip()
            if not cleaned:
                continue
            if self._contains_number(cleaned):
                result.append(cleaned)
                continue
            context = self._numeric_context_for_item(cleaned, feature_snapshot)
            result.append(f"{cleaned} ({context})" if context else cleaned)
        return result

    def _fallback_explanation(
        self,
        metrics: dict[str, Any],
        score: dict[str, Any],
    ) -> dict[str, Any]:
        score_value = int(score["credit_score"])
        band = self._score_band(score_value)
        late_ratio = float(metrics.get("late_payment_ratio") or 0.0)
        dti = float(metrics.get("debt_to_income_ratio") or 0.0)
        utilization = float(metrics.get("credit_utilization") or 0.0)
        account_age_days = int(metrics.get("account_age_days") or 0)
        total_payments = int(metrics.get("total_payments") or 0)
        late_payments = int(metrics.get("late_payments") or 0)
        max_days_past_due = int(metrics.get("max_days_past_due") or 0)
        stability = float(metrics.get("account_stability") or 0.0)

        strengths: list[str] = []
        risks: list[str] = []

        if late_ratio <= 0.03:
            strengths.append(f"Payment reliability is strong: {late_payments}/{total_payments} late ({late_ratio * 100:.1f}%).")
        elif late_ratio >= 0.1:
            risks.append(f"Late payments are elevated: {late_payments}/{total_payments} ({late_ratio * 100:.1f}%).")

        if dti <= 0.35:
            strengths.append(f"Debt is controlled relative to income (DTI {dti * 100:.1f}%).")
        elif dti > 0.45:
            risks.append(f"Debt-to-income is high at {dti * 100:.1f}%, which pressures your score.")
        else:
            risks.append(f"Debt-to-income is moderate at {dti * 100:.1f}% and limits score growth.")

        if 0 <= utilization <= 0.3:
            strengths.append(f"Credit utilization is healthy at {utilization * 100:.1f}%.")
        elif utilization > 0.5:
            risks.append(f"Credit utilization is high at {utilization * 100:.1f}%, a key drag on score.")

        if account_age_days >= 365 * 3:
            strengths.append(f"Account history is strong at {account_age_days / 365:.1f} years.")
        elif account_age_days < 365:
            risks.append(f"Account history is short at {account_age_days / 365:.1f} years.")

        if stability <= 5000:
            strengths.append(f"Balance volatility is stable (stability score {stability:.0f}).")

        if max_days_past_due > 30:
            risks.append(f"Maximum delinquency reached {max_days_past_due} days past due.")

        if not strengths:
            strengths.append("Your score has a solid base with room for targeted improvement.")
        if not risks:
            risks.append("No major risk spikes are visible, but consistency remains important.")

        recommendation = "Reduce revolving balances by 10-15% and keep all payments on time for the next two billing cycles."
        if dti > 0.5:
            recommendation = "Prioritize reducing debt by at least 15% over the next 60 days to improve risk profile."
        elif late_ratio > 0.08:
            recommendation = "Set up autopay and payment reminders to eliminate late payments over the next 90 days."
        elif utilization > 0.5:
            recommendation = "Bring utilization below 30% of limit to unlock noticeable score improvement."

        confidence_note = (
            "Deterministic explanation (AI key not configured)."
            if not self.enabled
            else "Fallback explanation used because live AI generation was unavailable."
        )

        return {
            "headline": f"Your credit score is {band} ({score_value}).",
            "summary": (
                f"Key metrics: late payments {late_payments}/{total_payments} ({late_ratio * 100:.1f}%), "
                f"DTI {dti * 100:.1f}%, utilization {utilization * 100:.1f}%, "
                f"account age {account_age_days / 365:.1f} years."
            ),
            "strengths": strengths[:3],
            "risk_factors": risks[:3],
            "recommendation": recommendation,
            "confidence_note": confidence_note,
        }

    def _fallback_advisor(
        self,
        metrics: dict[str, Any],
        history: list[dict[str, Any]],
        explanation: dict[str, Any],
    ) -> dict[str, Any]:
        dti = float(metrics.get("debt_to_income_ratio") or 0.0)
        late_ratio = float(metrics.get("late_payment_ratio") or 0.0)
        utilization = float(metrics.get("credit_utilization") or 0.0)

        actions: list[dict[str, str]] = []
        if utilization > 0.45:
            actions.append(
                {
                    "action": "Lower credit utilization below 30% of your total limit.",
                    "impact": "high",
                    "expected_outcome": "Improves your score sensitivity quickly by lowering perceived revolving risk.",
                    "timeline": "30-45 days",
                }
            )
        if late_ratio > 0.05:
            actions.append(
                {
                    "action": "Eliminate late payments using autopay and a weekly payment check routine.",
                    "impact": "high",
                    "expected_outcome": "Reduces negative payment events and stabilizes scoring trend.",
                    "timeline": "30-90 days",
                }
            )
        if dti > 0.4:
            actions.append(
                {
                    "action": "Reduce total debt by 10-15%, starting with highest-interest balances.",
                    "impact": "high",
                    "expected_outcome": "Lowers debt burden and improves affordability indicators.",
                    "timeline": "60-90 days",
                }
            )

        if not actions:
            actions.append(
                {
                    "action": "Maintain on-time payments and keep utilization between 10% and 30%.",
                    "impact": "medium",
                    "expected_outcome": "Supports gradual upward score momentum.",
                    "timeline": "ongoing",
                }
            )

        momentum_message = "Your current trajectory can improve further with consistent payment discipline."
        if len(history) >= 2:
            change = history[-1]["credit_score"] - history[0]["credit_score"]
            if change >= 20:
                momentum_message = "Your score momentum is positive. Keep current habits to sustain growth."
            elif change <= -20:
                momentum_message = "Your score trend is declining. Prioritize the top two actions immediately."

        return {
            "overview": explanation["summary"],
            "prioritized_actions": actions[:4],
            "momentum_message": momentum_message,
        }

    def _generate_llm_explanation(
        self,
        feature_snapshot: dict[str, Any],
    ) -> dict[str, Any] | None:
        system_prompt = (
            "You are a senior credit analyst writing concise, trustworthy explanations for customers and banks. "
            "Respond ONLY in JSON with keys: headline, summary, strengths, risk_factors, recommendation, confidence_note. "
            "strengths and risk_factors must be arrays of short bullet strings, each including concrete numbers from the input when applicable."
        )
        user_prompt = (
            "Using the following credit features, explain the score clearly in plain language. "
            "Keep an encouraging but realistic tone, avoid legal disclaimers, and keep it specific.\n\n"
            f"Input JSON:\n{json.dumps(feature_snapshot, indent=2)}"
        )
        return self._call_openai_json(system_prompt=system_prompt, user_prompt=user_prompt)

    def _generate_llm_advisor(
        self,
        feature_snapshot: dict[str, Any],
        score_history: list[dict[str, Any]],
        contributors: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        system_prompt = (
            "You are a personal credit improvement advisor. "
            "Respond ONLY in JSON with keys: overview, prioritized_actions, momentum_message. "
            "prioritized_actions must be an array of objects with keys: action, impact, expected_outcome, timeline. "
            "impact must be one of: high, medium, low."
        )
        user_prompt = (
            "Create a personalized credit improvement action plan from this data. "
            "Prioritize actions with highest score impact and make them concrete.\n\n"
            f"Feature snapshot:\n{json.dumps(feature_snapshot, indent=2, default=str)}\n\n"
            f"Score history (latest at end):\n{json.dumps(score_history, indent=2, default=str)}\n\n"
            f"Top contributor events:\n{json.dumps(contributors[:8], indent=2, default=str)}"
        )
        return self._call_openai_json(system_prompt=system_prompt, user_prompt=user_prompt)

    def build_insights(
        self,
        customer_id: int,
        score: dict[str, Any],
        metrics: dict[str, Any],
        history: list[dict[str, Any]],
        contributors: list[dict[str, Any]],
    ) -> dict[str, Any]:
        cache_key = (
            f"{customer_id}:"
            f"{score['credit_score']}:"
            f"{score['default_probability']}:"
            f"{metrics.get('late_payment_ratio', 0)}:"
            f"{metrics.get('debt_to_income_ratio', 0)}:"
            f"{metrics.get('credit_utilization', 0)}"
        )
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        feature_snapshot = self._feature_snapshot(metrics=metrics, score=score)

        explanation = self._fallback_explanation(metrics=metrics, score=score)
        advisor = self._fallback_advisor(metrics=metrics, history=history, explanation=explanation)

        llm_explanation = self._generate_llm_explanation(feature_snapshot=feature_snapshot)
        if isinstance(llm_explanation, dict):
            strengths = [
                str(item) for item in (llm_explanation.get("strengths") or explanation["strengths"])
            ][:4]
            risk_factors = [
                str(item) for item in (llm_explanation.get("risk_factors") or explanation["risk_factors"])
            ][:4]
            strengths = self._ensure_numeric_context(strengths, feature_snapshot)
            risk_factors = self._ensure_numeric_context(risk_factors, feature_snapshot)
            explanation = {
                "headline": str(llm_explanation.get("headline") or explanation["headline"]),
                "summary": str(llm_explanation.get("summary") or explanation["summary"]),
                "strengths": strengths,
                "risk_factors": risk_factors,
                "recommendation": str(llm_explanation.get("recommendation") or explanation["recommendation"]),
                "confidence_note": str(
                    llm_explanation.get("confidence_note")
                    or "AI-generated summary based on your latest credit behavior."
                ),
            }

        llm_advisor = self._generate_llm_advisor(
            feature_snapshot=feature_snapshot,
            score_history=history,
            contributors=contributors,
        )
        if isinstance(llm_advisor, dict):
            raw_actions = llm_advisor.get("prioritized_actions") or advisor["prioritized_actions"]
            actions = []
            for item in raw_actions:
                if not isinstance(item, dict):
                    continue
                impact = str(item.get("impact") or "medium").lower()
                if impact not in {"high", "medium", "low"}:
                    impact = "medium"
                actions.append(
                    {
                        "action": str(item.get("action") or ""),
                        "impact": impact,
                        "expected_outcome": str(item.get("expected_outcome") or ""),
                        "timeline": str(item.get("timeline") or "30-90 days"),
                    }
                )
            actions = [a for a in actions if a["action"]] or advisor["prioritized_actions"]
            advisor = {
                "overview": str(llm_advisor.get("overview") or advisor["overview"]),
                "prioritized_actions": actions[:5],
                "momentum_message": str(llm_advisor.get("momentum_message") or advisor["momentum_message"]),
            }

        bundle = {
            "score_explanation": explanation,
            "improvement_advisor": advisor,
        }
        self._cache_set(cache_key, bundle)
        return bundle
