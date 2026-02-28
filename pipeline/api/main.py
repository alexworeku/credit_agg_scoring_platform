import os
from pathlib import Path
from typing import Dict

import pyarrow.dataset as ds
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class CreditScoreResponse(BaseModel):
    customer_id: int
    credit_score: int
    risk_level: str
    default_probability: float


SCORES_PATH = Path(os.getenv("SCORES_PATH", "data/medallion/gold/customer_credit_scores"))
app = FastAPI(title="Credit Aggregation & Scoring API", version="1.0.0")
_scores_cache: Dict[int, CreditScoreResponse] = {}


def load_scores() -> None:
    global _scores_cache
    if not SCORES_PATH.exists():
        _scores_cache = {}
        return

    table = ds.dataset(str(SCORES_PATH), format="parquet").to_table(
        columns=["customer_id", "credit_score", "risk_level", "default_probability"]
    )

    data = {}
    for row in table.to_pylist():
        data[int(row["customer_id"])] = CreditScoreResponse(
            customer_id=int(row["customer_id"]),
            credit_score=int(row["credit_score"]),
            risk_level=str(row["risk_level"]),
            default_probability=float(row["default_probability"]),
        )

    _scores_cache = data


@app.on_event("startup")
def startup_event() -> None:
    load_scores()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "loaded_scores": len(_scores_cache)}


@app.get("/credit-score/{customer_id}", response_model=CreditScoreResponse)
def get_credit_score(customer_id: int) -> CreditScoreResponse:
    if customer_id not in _scores_cache:
        raise HTTPException(status_code=404, detail=f"customer_id {customer_id} not found")
    return _scores_cache[customer_id]
