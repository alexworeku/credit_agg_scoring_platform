# Credit Aggregation & Scoring Platform (Spark MVP)

![Demo](https://drive.google.com/uc?export=view&id=1Zs5Jp_jFtz9c3MBqREOi6feIFvTW5gZg)


This implementation follows a medallion architecture:

1. Bronze: raw source records + metadata
2. Silver: normalized unified financial model plus canonical `transactions`
3. Gold: customer credit features for the offline feature store
4. ML: batch training and batch scoring
5. Online inference: operational feature store + deployed model
6. API: FastAPI credit score endpoint

## Architecture

The codebase is split into two paths:

- Batch training pipeline:
  Bronze -> Silver -> Gold -> offline feature store -> model training -> model artifacts -> batch scoring
- Real-time inference pipeline:
  new transaction -> online Silver transaction store -> online feature store update -> deployed model inference -> current score + history

Design constraints:

- Silver is the institution-agnostic unified layer.
- `institution_id`, `source_system`, and canonical transaction fields are carried through Bronze, Silver, Gold, and online storage.
- Gold features are trained from complete historical data.
- Real-time inference uses the deployed model and the online feature store, not retraining logic or heuristic score deltas.

## Folder Layout

- `pipeline/ingestion/ingest_homecredit.py`
- `pipeline/bronze/bronze_schema.sql`
- `pipeline/silver/silver_transform.py`
- `pipeline/silver/silver_schema.sql`
- `pipeline/gold/feature_engineering.py`
- `pipeline/gold/gold_schema.sql`
- `pipeline/ml/train_model.py`
- `pipeline/ml/score_customers.py`
- `pipeline/api/main.py`

## Prerequisites

- Python 3.10+
- Java 8/11 (for Spark)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run Pipeline

From repo root:

```bash
python -m pipeline.ingestion.ingest_homecredit
python -m pipeline.silver.silver_transform
python -m pipeline.gold.feature_engineering
python -m pipeline.ml.train_model
python -m pipeline.ml.score_customers
```

Outputs are written under `data/medallion` and model artifacts under `artifacts/`.

Date note:
- Silver `payment_date`/`snapshot_date` are derived from Bronze relative offsets (`DAYS_*`, `MONTHS_BALANCE`)
  anchored to Bronze `ingestion_timestamp` by default.
- You can force a fixed anchor date with `PIPELINE_REFERENCE_DATE=YYYY-MM-DD` when running Silver transform.

## Serve API

```bash
uvicorn pipeline.api.main:app --reload --port 8000
```

Endpoint:

- `GET /credit-score/{customer_id}`

Example response:

```json
{
  "customer_id": 12345,
  "credit_score": 720,
  "risk_level": "LOW",
  "default_probability": 0.14
}
```

## Customer Portal Project (Frontend + API)

A separate customer-facing project is available under `customer_portal/` with:

- login-based customer self-service frontend
- personal score details and explainability factors
- score history trend and contributing transactions
- recommended personal actions to improve score
- OpenAI-powered credit score explanations and improvement advisor
- optional operational DB mode (for example PostgreSQL)

Run directly from parquet outputs:

```bash
npm --prefix customer_portal/frontend_react install
npm --prefix customer_portal/frontend_react run build
export OPENAI_API_KEY=your_openai_api_key
uvicorn customer_portal.backend.app:app --reload --port 8100
```

Open `http://localhost:8100`.

Note: customer portal transactions are sourced from the canonical Silver transaction dataset
`data/medallion/silver/transactions` by default.

Default demo login (if no user exists and demo seeding is enabled):

- username: `demo`
- password: `demo123`

Create real portal users:

```bash
python -m customer_portal.backend.manage_users create \
  --username alice \
  --customer-id 100002 \
  --full-name "Alice Doe"
```

To use an operational DB (recommended for production-like serving), load data first:

```bash
python -m customer_portal.backend.load_operational_db \
  --database-url postgresql+psycopg://user:password@localhost:5432/credit_portal \
  --replace
```

The loader uses performance-friendly defaults for local runs (3-year transaction lookback and SQLite fast mode).

Then point API to DB:

```bash
export OPERATIONAL_DB_URL=postgresql+psycopg://user:password@localhost:5432/credit_portal
uvicorn customer_portal.backend.app:app --reload --port 8100
```

## Realtime Simulation Demo (Persistent)

The portal now includes a `Simulator` page in the top navigation (requires operational DB mode).

What it does on submit:

1. Stores raw event in `bronze_simulated_transactions`
2. Transforms and stores normalized event in `silver_simulated_transactions`
3. Loads event into the online Silver table `customer_transactions`
4. Recomputes the customer row in the online feature store `customer_credit_features`
5. Scores with the deployed model artifact
6. Appends to `customer_credit_score_history` for score trend persistence

This is persisted in your operational database (`customer_portal.db` for SQLite, or PostgreSQL if configured).

Demo flow:

```bash
# 1) Ensure operational DB is loaded and used
python -m customer_portal.backend.load_operational_db --database-url sqlite:///customer_portal.db --replace
export OPERATIONAL_DB_URL=sqlite:///customer_portal.db

# 2) Start portal
uvicorn customer_portal.backend.app:app --reload --port 8100
```

Then in UI:

1. Open `Simulator`
2. Pick transaction type, amount, expected amount, status, date, etc.
3. Submit transaction
4. Verify dashboard updates immediately:
   - current score changes
   - score trend gets a new historical point
   - recent transactions includes the simulated event
   - feature cards/metrics refresh

## Multi-Institution Fields

All Bronze/Silver/Gold tables include:

- `institution_id`
- `source_system`
- `customer_id`

Defaults for Home Credit ingestion:

- `institution_id=home_credit`
- `source_system=home_credit`

Override with environment variables if needed:

```bash
export INSTITUTION_ID=cbe
export SOURCE_SYSTEM=cbe
```
