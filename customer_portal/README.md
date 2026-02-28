# Customer Credit Portal

Customer-facing self-service project with:

- Login-based customer dashboard
- Personal profile + current credit score
- Recent transactions + transactions contributing to score
- Credit score history from model score records
- Personalized recommendations / action plan
- AI credit score explanation (LLM-powered)
- AI credit improvement advisor (LLM-powered)
- Optional operational database loader for production-style serving (e.g., PostgreSQL)

## What it uses

- Frontend: React + Vite (built assets served by FastAPI)
- Backend: FastAPI
- Data source mode 1 (default): Parquet from `data/medallion`
- Data source mode 2 (optional): SQL operational database (`OPERATIONAL_DB_URL`)

By default, portal transaction endpoints are sourced from Silver structured tables
(processed from Bronze):

- `data/medallion/silver/transactions`

Override paths if needed:

```bash
export PORTAL_TRANSACTIONS_PATH=/absolute/or/relative/path
```

## Enable OpenAI-powered insights

Set your API key before running the backend:

```bash
export OPENAI_API_KEY=your_openai_api_key
```

Optional:

```bash
export OPENAI_MODEL=gpt-4o-mini
```

If `OPENAI_API_KEY` is not set, the portal uses deterministic fallback insights.

## Run (Parquet mode)

From repo root:

```bash
npm --prefix customer_portal/frontend_react install
npm --prefix customer_portal/frontend_react run build
uvicorn customer_portal.backend.app:app --reload --port 8100
```

Open:

- `http://localhost:8100/`

By default, a demo user is auto-seeded on first startup if no user exists:

- `username: demo`
- `password: demo123`

Disable that behavior:

```bash
export PORTAL_SEED_DEMO_USER=false
```

## Create portal users

Create a login mapped to a specific `customer_id`:

```bash
python -m customer_portal.backend.manage_users create \
  --username alice \
  --customer-id 100002 \
  --full-name "Alice Doe"
```

List users:

```bash
python -m customer_portal.backend.manage_users list
```

## API highlights

- `POST /api/auth/login`
- `GET /api/auth/me`
- `GET /api/me/dashboard`
- `GET /api/me/transactions`
- `GET /api/me/score-history`
- `GET /api/me/score-contributors`
- `GET /api/me/credit-plan`
- `GET /api/me/ai/score-explanation`
- `GET /api/me/ai/improvement-advisor`
- `GET /api/me/ai/insights`

## Load operational DB (recommended for production)

Load customer profile, loan, score, feature, and canonical transaction data into SQL tables:

```bash
python -m customer_portal.backend.load_operational_db \
  --database-url postgresql+psycopg://user:password@localhost:5432/credit_portal \
  --replace
```

Performance defaults (for faster local loads):

- `--batch-size 20000`
- `--transaction-lookback-days 1095` (3 years from latest score date)
- SQLite fast mode PRAGMAs are enabled unless `--disable-sqlite-fast-mode` is set

Then run backend using DB mode:

```bash
export OPERATIONAL_DB_URL=postgresql+psycopg://user:password@localhost:5432/credit_portal
uvicorn customer_portal.backend.app:app --reload --port 8100
```

Note:

- If `OPERATIONAL_DB_URL` is set, the API reads from DB tables.
- If not set, it reads directly from parquet datasets.
- Real-time simulation updates the online feature store tables (`customer_transactions`, `customer_credit_features`, `customer_credit_scores`) and scores with the deployed model artifact.
