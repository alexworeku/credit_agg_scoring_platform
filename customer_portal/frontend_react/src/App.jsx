import { useEffect, useMemo, useRef, useState } from "react";
import ReactApexChart from "react-apexcharts";
import { Navigate, NavLink, Route, Routes, useNavigate } from "react-router-dom";

const TOKEN_KEY = "credit_portal_token";

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token);
}

function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

async function apiRequest(path, token, options = {}) {
  const headers = new Headers(options.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (!headers.has("Content-Type") && options.body) headers.set("Content-Type", "application/json");

  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function parseHistoryTimestamp(point) {
  const rawValue = point?.score_generated_at || point?.period;
  if (!rawValue) return null;
  const parsed = new Date(rawValue);
  if (!Number.isNaN(parsed.getTime())) return parsed;
  if (typeof rawValue === "string" && /^\d{4}-\d{2}$/.test(rawValue)) {
    const fallback = new Date(`${rawValue}-01`);
    if (!Number.isNaN(fallback.getTime())) return fallback;
  }
  return null;
}

function bucketKeyForDate(date, granularity) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  const hour = `${date.getHours()}`.padStart(2, "0");
  const minute = `${date.getMinutes()}`.padStart(2, "0");
  if (granularity === "year") return `${year}`;
  if (granularity === "month") return `${year}-${month}`;
  if (granularity === "day") return `${year}-${month}-${day}`;
  return `${year}-${month}-${day}T${hour}:${minute}`;
}

function labelForBucket(date, granularity) {
  if (granularity === "year") {
    return date.toLocaleDateString("en-US", { year: "numeric" });
  }
  if (granularity === "month") {
    return date.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  }
  if (granularity === "day") {
    return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  }
  return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
}

function tooltipLabel(date, granularity) {
  if (granularity === "year") {
    return date.toLocaleDateString("en-US", { year: "numeric" });
  }
  if (granularity === "month") {
    return date.toLocaleDateString("en-US", { month: "long", year: "numeric" });
  }
  if (granularity === "day") {
    return date.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
  }
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function buildChartHistory(history) {
  const enriched = (history || [])
    .map((point) => {
      const timestamp = parseHistoryTimestamp(point);
      return timestamp ? { ...point, timestamp } : null;
    })
    .filter(Boolean)
    .sort((left, right) => left.timestamp - right.timestamp);

  if (enriched.length === 0) {
    return { granularity: "month", points: [] };
  }

  const uniqueYears = new Set(enriched.map((item) => `${item.timestamp.getFullYear()}`));
  const uniqueMonths = new Set(
    enriched.map((item) => `${item.timestamp.getFullYear()}-${item.timestamp.getMonth()}`)
  );
  const uniqueDays = new Set(
    enriched.map((item) => `${item.timestamp.getFullYear()}-${item.timestamp.getMonth()}-${item.timestamp.getDate()}`)
  );

  let granularity = "time";
  if (uniqueYears.size > 1) granularity = "year";
  else if (uniqueMonths.size > 1) granularity = "month";
  else if (uniqueDays.size > 1) granularity = "day";

  const grouped = new Map();
  for (const item of enriched) {
    grouped.set(bucketKeyForDate(item.timestamp, granularity), item);
  }

  const points = [...grouped.values()].map((item) => ({
    ...item,
    label: labelForBucket(item.timestamp, granularity),
    tooltipLabel: tooltipLabel(item.timestamp, granularity),
  }));

  return { granularity, points };
}

function formatPercent(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function formatAgeLabel(days) {
  const totalDays = Number(days || 0);
  if (!Number.isFinite(totalDays) || totalDays <= 0) return "New profile";
  const years = totalDays / 365;
  if (years >= 1) return `${years.toFixed(years >= 5 ? 0 : 1)} years`;
  return `${Math.round(totalDays)} days`;
}

function scoreBand(score) {
  const numeric = Number(score || 0);
  if (numeric >= 800) return { label: "Exceptional", tone: "good", slug: "exceptional" };
  if (numeric >= 740) return { label: "Very Good", tone: "good", slug: "very-good" };
  if (numeric >= 670) return { label: "Good", tone: "good", slug: "good" };
  if (numeric >= 580) return { label: "Fair", tone: "warning", slug: "fair" };
  return { label: "Needs Work", tone: "danger", slug: "needs-work" };
}

function initialsFromUser(user) {
  const fullName = user?.full_name?.trim();
  if (!fullName) return user?.username?.slice(0, 2).toUpperCase() || "CU";
  return fullName
    .split(" ")
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "")
    .join("");
}

function scoreChange(history) {
  if (!history || history.length < 2) return null;
  const current = Number(history[history.length - 1]?.credit_score || 0);
  const prior = Number(history[history.length - 2]?.credit_score || 0);
  return current - prior;
}

function classifyFactorCards(metrics) {
  const lateRatio = Number(metrics?.late_payment_ratio || 0);
  const latePayments = Number(metrics?.late_payments || 0);
  const totalPayments = Number(metrics?.total_payments || 0);
  const utilization = Number(metrics?.credit_utilization || 0);
  const accountAgeDays = Number(metrics?.account_age_days || 0);
  const ageYears = Math.max(0, Math.round(accountAgeDays / 365));
  const debtRatio = Number(metrics?.debt_to_income_ratio || 0);

  const payment =
    lateRatio <= 0.03 ? { value: "Excellent", tone: "good" } : lateRatio <= 0.08 ? { value: "Good", tone: "warning" } : { value: "Needs Work", tone: "danger" };

  const util =
    utilization <= 0.1
      ? { value: "Very Low", tone: "good" }
      : utilization <= 0.3
      ? { value: "Healthy", tone: "good" }
      : utilization <= 0.5
      ? { value: "Moderate", tone: "warning" }
      : { value: "High", tone: "danger" };

  const age = ageYears >= 7 ? { value: `${ageYears} years`, tone: "good" } : ageYears >= 3 ? { value: `${ageYears} years`, tone: "warning" } : { value: `${ageYears} years`, tone: "danger" };

  const debt =
    debtRatio <= 0.3
      ? { value: "Low", tone: "good" }
      : debtRatio <= 0.45
      ? { value: "Moderate", tone: "warning" }
      : { value: "High", tone: "danger" };

  return [
    {
      label: "Payment History",
      metric: `Late ${latePayments}/${totalPayments} (${(lateRatio * 100).toFixed(1)}%)`,
      ...payment,
    },
    {
      label: "Credit Utilization",
      metric: `${(utilization * 100).toFixed(1)}% of limit`,
      ...util,
    },
    {
      label: "Account Age",
      metric: `${Math.max(0, Math.round(accountAgeDays)).toLocaleString()} days`,
      ...age,
    },
    {
      label: "Debt Ratio",
      metric: `DTI ${(debtRatio * 100).toFixed(1)}%`,
      ...debt,
    },
  ];
}

function compareValues(a, b) {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b));
}

function formatSignedNumber(value) {
  const number = Number(value || 0);
  if (number > 0) return `+${number}`;
  return `${number}`;
}

function transactionDetail(tx) {
  const type = String(tx.transaction_type || "").toUpperCase();
  const status = String(tx.status || "").toUpperCase();
  const daysPastDue = Number(tx.days_past_due || 0);

  if (type === "LOAN_PAYMENT") {
    if (daysPastDue > 0 || status === "LATE" || status === "PAST_DUE") {
      return `${daysPastDue} days past due`;
    }
    if (tx.expected_amount) return `Scheduled ${formatCurrency(tx.expected_amount)}`;
    return "Reported on-time payment";
  }

  if (type === "BALANCE_SNAPSHOT") {
    if (tx.expected_amount && Number(tx.expected_amount) > 0) {
      const usage = Number(tx.amount || 0) / Number(tx.expected_amount);
      return `Using ${formatPercent(usage, 0)} of reported limit`;
    }
    if (daysPastDue > 0) return `${daysPastDue} days past due`;
    return "Reported balance update";
  }

  return "Reported account activity";
}

function normalizeTransactionEvent(tx) {
  const status = String(tx.status || "").toUpperCase();
  const type = String(tx.transaction_type || "").toUpperCase();
  const amount = Number(tx.amount || 0);
  const expected = Number(tx.expected_amount || 0);

  if (status === "LATE" || status === "PAST_DUE") {
    return {
      icon: "↓",
      tone: "negative",
      label: "Late payment",
      detail: `${tx.days_past_due || 0} days past due`,
    };
  }

  if (type === "LOAN_PAYMENT") {
    return {
      icon: "↑",
      tone: "positive",
      label: "On-time payment",
      detail: "Payment completed on time",
    };
  }

  if (type === "BALANCE_SNAPSHOT") {
    if (expected > 0 && amount / expected < 0.3) {
      return {
        icon: "↑",
        tone: "positive",
        label: "Credit utilization reduced",
        detail: "Balance remains within healthy limits",
      };
    }
    return {
      icon: "•",
      tone: "neutral",
      label: "Balance updated",
      detail: "Current balance snapshot recorded",
    };
  }

  return {
    icon: "•",
    tone: "neutral",
    label: "Transaction recorded",
    detail: "General account activity",
  };
}

function LoginView({ onLogin, error, busy }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const submit = async (event) => {
    event.preventDefault();
    await onLogin(username, password);
  };

  return (
    <div className="auth-shell">
      <div className="auth-card">
        <h1>Welcome back</h1>
        <p className="auth-muted">Sign in to access your credit intelligence dashboard.</p>
        <form onSubmit={submit} className="auth-form">
          <label htmlFor="username">Username</label>
          <input
            id="username"
            type="text"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            required
            autoComplete="username"
          />
          <label htmlFor="password">Password</label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
            autoComplete="current-password"
          />
          <button type="submit" className="primary-btn" disabled={busy}>
            {busy ? "Signing in..." : "Continue"}
          </button>
        </form>
        {error ? <p className="error-text">{error}</p> : null}
      </div>
    </div>
  );
}

function ProfileMenu({ user, onLogout }) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef(null);

  useEffect(() => {
    const listener = (event) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target)) setOpen(false);
    };
    window.addEventListener("click", listener);
    return () => window.removeEventListener("click", listener);
  }, []);

  return (
    <div className="profile-menu-wrap" ref={wrapRef}>
      <button className="profile-toggle" type="button" onClick={() => setOpen((prev) => !prev)}>
        <span className="avatar-chip">{initialsFromUser(user)}</span>
        <span className="profile-name">{user?.full_name || user?.username || "Profile"}</span>
      </button>
      {open ? (
        <div className="profile-dropdown">
          <NavLink to="/credit-profile" className="dropdown-item" onClick={() => setOpen(false)}>
            Credit Profile
          </NavLink>
          <button
            type="button"
            className="dropdown-item danger"
            onClick={() => {
              setOpen(false);
              onLogout();
            }}
          >
            Logout
          </button>
        </div>
      ) : null}
    </div>
  );
}

function PortalLayout({ user, onLogout, children }) {
  return (
    <div className="portal-shell">
      <header className="portal-header">
        <div className="brand-wrap">
          <span className="brand-mark" />
          <span className="brand-name">Credit Intelligence</span>
        </div>
        <nav className="header-nav" aria-label="Primary">
          <NavLink to="/" end className={({ isActive }) => (isActive ? "header-link active" : "header-link")}>
            Dashboard
          </NavLink>
          <NavLink to="/credit-profile" className={({ isActive }) => (isActive ? "header-link active" : "header-link")}>
            Credit Profile
          </NavLink>
          <NavLink to="/transactions" className={({ isActive }) => (isActive ? "header-link active" : "header-link")}>
            Activity
          </NavLink>
          <NavLink to="/simulator" className={({ isActive }) => (isActive ? "header-link active" : "header-link")}>
            Simulator
          </NavLink>
        </nav>
        <ProfileMenu user={user} onLogout={onLogout} />
      </header>
      <main className="portal-content">{children}</main>
    </div>
  );
}

function TrendChart({ history, currentScore, monthlyChange }) {
  const chartHistory = useMemo(() => buildChartHistory(history), [history]);
  const points = chartHistory.points;

  if (!points || points.length === 0) {
    return (
      <section className="section-card">
        <div className="section-header">
          <h2>Score Trend</h2>
        </div>
        <div className="empty-inline">No history available.</div>
      </section>
    );
  }

  const series = useMemo(
    () => [
      {
        name: "Credit Score",
        data: points.map((point) => ({
          x: point.timestamp.getTime(),
          y: Number(point.credit_score || 0),
          metaLabel: point.tooltipLabel,
        })),
      },
    ],
    [points]
  );

  const chartOptions = useMemo(
    () => ({
      chart: {
        id: "credit-score-trend",
        type: "line",
        zoom: {
          enabled: true,
          type: "x",
          autoScaleYaxis: true,
        },
        toolbar: {
          show: true,
          tools: {
            download: false,
            selection: true,
            zoom: true,
            zoomin: true,
            zoomout: true,
            pan: true,
            reset: true,
          },
        },
        animations: {
          easing: "easeinout",
          speed: 280,
        },
        fontFamily: "Manrope, Inter, sans-serif",
      },
      stroke: {
        curve: "straight",
        width: 3,
      },
      markers: {
        size: 4,
        strokeWidth: 0,
        hover: {
          sizeOffset: 2,
        },
        colors: ["#12382b"],
      },
      grid: {
        borderColor: "#dbe5dd",
        strokeDashArray: 4,
        padding: {
          left: 12,
          right: 18,
        },
      },
      fill: {
        type: "gradient",
        gradient: {
          shadeIntensity: 1,
          opacityFrom: 0.28,
          opacityTo: 0.04,
          stops: [0, 100],
        },
      },
      xaxis: {
        type: "datetime",
        tickAmount: Math.min(6, Math.max(2, points.length - 1)),
        labels: {
          style: {
            colors: "#678473",
            fontSize: "11px",
          },
          formatter: (value) => labelForBucket(new Date(Number(value)), chartHistory.granularity),
        },
        axisBorder: {
          show: false,
        },
        axisTicks: {
          show: false,
        },
      },
      yaxis: {
        min: 300,
        max: 850,
        tickAmount: 6,
        labels: {
          style: {
            colors: "#678473",
            fontSize: "11px",
          },
          formatter: (value) => Math.round(value).toString(),
        },
      },
      tooltip: {
        theme: "light",
        x: {
          formatter: (_, { dataPointIndex }) => points[dataPointIndex]?.tooltipLabel || "",
        },
        y: {
          formatter: (value) => `Score: ${Math.round(value)}`,
        },
      },
      colors: ["#12382b"],
      dataLabels: {
        enabled: false,
      },
      states: {
        hover: {
          filter: {
            type: "darken",
            value: 0.9,
          },
        },
      },
    }),
    [chartHistory.granularity, points]
  );

  return (
    <section className="section-card">
      <div className="section-header">
        <h2>Score Trend</h2>
        <div className="trend-summary">
          <strong>{currentScore}</strong>
          {monthlyChange === null ? (
            <span className="delta neutral">No prior model score</span>
          ) : (
            <span className={monthlyChange >= 0 ? "delta positive" : "delta negative"}>
              {monthlyChange >= 0 ? "↑" : "↓"} {monthlyChange >= 0 ? "+" : ""}
              {monthlyChange} from prior period
            </span>
          )}
        </div>
      </div>
      <p className="section-helper">
        {chartHistory.granularity === "year"
          ? "Year-level score movement based on reported score updates."
          : chartHistory.granularity === "month"
          ? "Month-level score movement based on reported score updates."
          : chartHistory.granularity === "day"
          ? "Day-level score movement within the current month."
          : "Intraday score movement for the current day."}
      </p>
      <div className="trend-chart-wrap">
        <ReactApexChart options={chartOptions} series={series} type="area" height={260} />
      </div>
    </section>
  );
}

function WhatIfCalculator({ token, simulatorStatus, currentScore, inModal = false, onClose = null }) {
  const [form, setForm] = useState({
    transaction_type: "LOAN_PAYMENT",
    amount: "",
    expected_amount: "",
    days_past_due: "0",
    status: "ON_TIME",
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  useEffect(() => {
    setForm((prev) => {
      const statusOptions =
        prev.transaction_type === "LOAN_PAYMENT" ? ["ON_TIME", "LATE", "PAST_DUE"] : ["CURRENT", "PAST_DUE"];
      return {
        ...prev,
        status: statusOptions.includes(prev.status) ? prev.status : statusOptions[0],
      };
    });
  }, [form.transaction_type]);

  const statusOptions =
    form.transaction_type === "LOAN_PAYMENT" ? ["ON_TIME", "LATE", "PAST_DUE"] : ["CURRENT", "PAST_DUE"];
  const amountPresets = form.transaction_type === "LOAN_PAYMENT" ? [65, 120, 245, 480] : [150, 320, 650, 1200];

  const setField = (field, value) => {
    setForm((prev) => ({ ...prev, [field]: value }));
    setError("");
  };

  const submitWhatIf = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");

    const amount = Number(form.amount);
    const expectedAmount = form.expected_amount.trim() ? Number(form.expected_amount) : null;
    const daysPastDue = Number(form.days_past_due || "0");

    if (!Number.isFinite(amount)) {
      setBusy(false);
      setError("Amount must be numeric.");
      return;
    }
    if (expectedAmount !== null && !Number.isFinite(expectedAmount)) {
      setBusy(false);
      setError("Expected amount must be numeric.");
      return;
    }
    if (!Number.isFinite(daysPastDue) || daysPastDue < 0) {
      setBusy(false);
      setError("Days past due must be zero or positive.");
      return;
    }

    try {
      const preview = await apiRequest("/api/me/what-if-score", token, {
        method: "POST",
        body: JSON.stringify({
          transaction_type: form.transaction_type,
          amount,
          expected_amount: expectedAmount,
          days_past_due: Math.round(daysPastDue),
          status: form.status,
        }),
      });
      setResult(preview);
    } catch (submitError) {
      setError(submitError.message || "Unable to calculate score preview.");
    } finally {
      setBusy(false);
    }
  };

  if (!simulatorStatus?.enabled) {
    return (
      <section className={inModal ? "whatif-modal-card" : "section-card whatif-card"}>
        <div className="section-header">
          <h2>What-If Score</h2>
          {onClose ? (
            <button type="button" className="icon-btn" onClick={onClose} aria-label="Close what-if calculator">
              ×
            </button>
          ) : null}
        </div>
        <p className="section-helper">
          {simulatorStatus?.reason || "What-if scoring is available only when the portal runs against the operational database."}
        </p>
      </section>
    );
  }

  return (
    <section className={inModal ? "whatif-modal-card whatif-card" : "section-card whatif-card"}>
      <div className="section-header">
        <h2>What-If Score</h2>
        {onClose ? (
          <button type="button" className="icon-btn" onClick={onClose} aria-label="Close what-if calculator">
            ×
          </button>
        ) : null}
      </div>
      <p className="section-helper">Preview the same score outcome you would get by submitting this exact transaction in the simulator.</p>
      <form className="whatif-form" onSubmit={submitWhatIf}>
        <div className="chip-group">
          {[
            { value: "LOAN_PAYMENT", label: "Loan Payment" },
            { value: "BALANCE_SNAPSHOT", label: "Balance Update" },
          ].map((option) => (
            <button
              key={option.value}
              type="button"
              className={form.transaction_type === option.value ? "chip active" : "chip"}
              onClick={() => setField("transaction_type", option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>

        <label className="whatif-field">
          <span>Amount</span>
          <input
            type="number"
            step="0.01"
            value={form.amount}
            onChange={(event) => setField("amount", event.target.value)}
            placeholder="0.00"
            required
          />
        </label>

        <div className="chip-group">
          {amountPresets.map((amount) => (
            <button key={amount} type="button" className="chip" onClick={() => setField("amount", String(amount))}>
              {formatCurrency(amount)}
            </button>
          ))}
        </div>

        <div className="whatif-grid">
          <label className="whatif-field">
            <span>{form.transaction_type === "LOAN_PAYMENT" ? "Expected Payment" : "Reported Limit"}</span>
            <input
              type="number"
              step="0.01"
              value={form.expected_amount}
              onChange={(event) => setField("expected_amount", event.target.value)}
              placeholder={form.transaction_type === "LOAN_PAYMENT" ? "Optional" : "Optional"}
            />
          </label>
          <label className="whatif-field">
            <span>Days Past Due</span>
            <input
              type="number"
              min="0"
              step="1"
              value={form.days_past_due}
              onChange={(event) => setField("days_past_due", event.target.value)}
            />
          </label>
        </div>

        <div className="chip-group">
          {statusOptions.map((statusOption) => (
            <button
              key={statusOption}
              type="button"
              className={form.status === statusOption ? "chip active" : "chip"}
              onClick={() => setField("status", statusOption)}
            >
              {statusOption.replace("_", " ")}
            </button>
          ))}
        </div>

        {error ? <p className="error-text">{error}</p> : null}

        <button type="submit" className="primary-btn" disabled={busy}>
          {busy ? "Calculating..." : "Calculate What-If Score"}
        </button>
      </form>

      <div className="whatif-result">
        <span>Current score</span>
        <strong>{currentScore}</strong>
        {result ? (
          <>
            <div className="whatif-result-top">
              <div>
                <span>Projected score</span>
                <strong>{result.projected_credit_score}</strong>
              </div>
              <div>
                <span>Change</span>
                <strong className={result.score_change >= 0 ? "delta positive" : "delta negative"}>
                  {result.score_change >= 0 ? "+" : ""}
                  {result.score_change}
                </strong>
              </div>
            </div>
            <dl className="whatif-result-grid">
              <dt>Status</dt>
              <dd>{String(result.status || "").replace("_", " ")}</dd>
              <dt>Expected</dt>
              <dd>{formatCurrency(result.expected_amount)}</dd>
              <dt>Risk</dt>
              <dd>{result.risk_level}</dd>
              <dt>Default prob.</dt>
              <dd>{formatPercent(result.projected_default_probability, 1)}</dd>
            </dl>
          </>
        ) : (
          <p className="section-helper">Enter a payment or balance update to preview the resulting score before applying it.</p>
        )}
      </div>
    </section>
  );
}

function DashboardPage({ dashboard, history, transactions, token, simulatorStatus, onViewAllTransactions, onViewCreditProfile }) {
  const [whatIfOpen, setWhatIfOpen] = useState(false);
  const metrics = dashboard.metrics || {};
  const factorCards = classifyFactorCards(metrics);
  const trendHistory = useMemo(() => buildChartHistory(history).points, [history]);
  const monthlyChange = scoreChange(trendHistory);
  const scoreSummary = scoreBand(dashboard.score.credit_score);
  const actionItems = (dashboard.recommendations || []).map((item) => item.action).filter(Boolean).slice(0, 3);
  const keyFactors = (dashboard.factors || []).slice(0, 4);
  const overviewItems = [
    { label: "Reported accounts", value: Number(metrics.total_accounts || 0).toLocaleString() },
    { label: "Active loans", value: Number(metrics.active_loans || 0).toLocaleString() },
    { label: "On-time payments", value: formatPercent(1 - Number(metrics.late_payment_ratio || 0), 0) },
    { label: "Last reported activity", value: formatDate(dashboard.transaction_summary?.latest_transaction_date) },
  ];

  const recentRows = useMemo(
    () =>
      (transactions || []).slice(0, 5).map((item) => ({
        ...item,
        normalized: normalizeTransactionEvent(item),
      })),
    [transactions]
  );

  return (
    <div className="dashboard-grid">
      <div className="dashboard-main">
        <section className={`hero-card hero-card-${scoreSummary.slug}`}>
          <div className="hero-top">
            <span className="hero-label">Credit Score</span>
            <div className="hero-tools">
              <span className={`risk-badge ${scoreSummary.tone}`}>
                {scoreSummary.label}
              </span>
              <button type="button" className="hero-whatif-btn" onClick={() => setWhatIfOpen(true)}>
                What-If Score
              </button>
            </div>
          </div>
          <div className="hero-score">{dashboard.score.credit_score}</div>
          {monthlyChange === null ? (
            <div className="hero-change neutral">No prior model score</div>
          ) : (
            <div className={monthlyChange >= 0 ? "hero-change positive" : "hero-change negative"}>
              {monthlyChange >= 0 ? "+" : ""}
              {monthlyChange} from prior score
            </div>
          )}
          <div className="hero-meta">
            <span>Updated {formatDate(dashboard.score.score_generated_at)}</span>
            <span>{dashboard.transaction_summary?.total_transactions || 0} reported events</span>
          </div>
          <div className="hero-scale" aria-hidden="true">
            <div className="hero-scale-bar">
              <div
                className="hero-scale-fill"
                style={{ width: `${Math.max(0, Math.min(100, ((Number(dashboard.score.credit_score) - 300) / 550) * 100))}%` }}
              />
            </div>
            <div className="hero-scale-labels">
              <span>300</span>
              <span>850</span>
            </div>
          </div>
        </section>

        <section className="factors-row">
          {factorCards.map((factor) => (
            <article key={factor.label} className="factor-card">
              <span className="factor-label">{factor.label}</span>
              <strong className={`factor-value ${factor.tone}`}>{factor.value}</strong>
              <span className="factor-metric">{factor.metric}</span>
            </article>
          ))}
        </section>

        <section className="section-card">
          <div className="section-header">
            <h2>What's Helping And Hurting</h2>
            <button type="button" className="text-btn" onClick={onViewCreditProfile}>
              View profile
            </button>
          </div>
          <div className="key-factor-list">
            {keyFactors.length === 0 ? <div className="empty-inline">No factors available.</div> : null}
            {keyFactors.map((item, idx) => (
              <div key={`${item.factor}-${idx}`} className={`key-factor-item ${item.impact}`}>
                <span className="impact-icon">{item.impact === "negative" ? "↓" : item.impact === "positive" ? "↑" : "•"}</span>
                <div className="key-factor-copy">
                  <strong>{item.factor}</strong>
                  <span>{item.detail}</span>
                  <small>Healthy benchmark: {item.benchmark}</small>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="section-card">
          <div className="section-header">
            <h2>Credit Overview</h2>
          </div>
          <div className="overview-grid">
            {overviewItems.map((item) => (
              <article key={item.label} className="overview-card">
                <span>{item.label}</span>
                <strong>{item.value}</strong>
              </article>
            ))}
          </div>
        </section>

        <TrendChart history={trendHistory} currentScore={dashboard.score.credit_score} monthlyChange={monthlyChange} />

        <section className="section-card">
          <div className="section-header">
            <h2>Recent Reported Activity</h2>
            <button type="button" className="text-btn" onClick={onViewAllTransactions}>
              View all
            </button>
          </div>
          <p className="section-helper">Recent payment and balance updates reported on your aggregated credit profile.</p>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Activity</th>
                  <th>Amount</th>
                  <th>Status</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {recentRows.map((item) => (
                  <tr key={item.transaction_id}>
                    <td>{formatDate(item.transaction_date)}</td>
                    <td>{item.normalized.label}</td>
                    <td>{formatCurrency(item.amount)}</td>
                    <td>
                      <span className={`status-pill ${item.normalized.tone}`}>{item.normalized.tone}</span>
                    </td>
                    <td>{transactionDetail(item)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <aside className="dashboard-side">
        <section className="section-card">
          <div className="section-header">
            <h2>Profile Snapshot</h2>
          </div>
          <dl className="snapshot-list">
            <div>
              <dt>Total debt</dt>
              <dd>{formatCurrency(metrics.total_debt)}</dd>
            </div>
            <div>
              <dt>Credit utilization</dt>
              <dd>{formatPercent(metrics.credit_utilization, 0)}</dd>
            </div>
            <div>
              <dt>Late or past-due events</dt>
              <dd>{dashboard.transaction_summary?.late_or_past_due_events || 0}</dd>
            </div>
            <div>
              <dt>Credit history length</dt>
              <dd>{formatAgeLabel(metrics.account_age_days)}</dd>
            </div>
          </dl>
        </section>
        <section className="section-card action-card">
          <div className="section-header">
            <h2>Recommended Actions</h2>
          </div>
          <ul className="action-list">
            {actionItems.map((item, idx) => (
              <li key={`${item}-${idx}`}>{item}</li>
            ))}
          </ul>
        </section>
      </aside>

      {whatIfOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setWhatIfOpen(false)}>
          <div className="modal-dialog" role="dialog" aria-modal="true" aria-label="What-if score calculator" onClick={(event) => event.stopPropagation()}>
            <WhatIfCalculator
              token={token}
              simulatorStatus={simulatorStatus}
              currentScore={dashboard.score.credit_score}
              inModal
              onClose={() => setWhatIfOpen(false)}
            />
          </div>
        </div>
      ) : null}
    </div>
  );
}

function TransactionsPage({ transactions, dashboard, onBack }) {
  const pageSize = 20;
  const [filter, setFilter] = useState("ALL");
  const [sortBy, setSortBy] = useState("transaction_date");
  const [sortDirection, setSortDirection] = useState("desc");
  const [page, setPage] = useState(1);
  const metrics = dashboard.metrics || {};
  const summary = dashboard.transaction_summary || {};

  const toggleSort = (nextSortBy) => {
    if (sortBy === nextSortBy) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setSortBy(nextSortBy);
    setSortDirection(nextSortBy === "transaction_date" ? "desc" : "asc");
  };

  const filtered = useMemo(() => {
    if (filter === "ALL") return transactions;
    return transactions.filter((item) => item.transaction_type === filter);
  }, [transactions, filter]);

  const sorted = useMemo(() => {
    const rows = [...filtered];
    rows.sort((left, right) => {
      const leftNormalized = normalizeTransactionEvent(left);
      const rightNormalized = normalizeTransactionEvent(right);

      const leftValue =
        sortBy === "transaction_date"
          ? new Date(left.transaction_date || "1970-01-01").getTime()
          : sortBy === "activity"
          ? leftNormalized.label
          : sortBy === "amount"
          ? Number(left.amount || 0)
          : sortBy === "expected_amount"
          ? Number(left.expected_amount || 0)
          : sortBy === "status"
          ? leftNormalized.tone
          : left.reference_id || "";

      const rightValue =
        sortBy === "transaction_date"
          ? new Date(right.transaction_date || "1970-01-01").getTime()
          : sortBy === "activity"
          ? rightNormalized.label
          : sortBy === "amount"
          ? Number(right.amount || 0)
          : sortBy === "expected_amount"
          ? Number(right.expected_amount || 0)
          : sortBy === "status"
          ? rightNormalized.tone
          : right.reference_id || "";

      const baseCompare = compareValues(leftValue, rightValue);
      return sortDirection === "asc" ? baseCompare : -baseCompare;
    });
    return rows;
  }, [filtered, sortBy, sortDirection]);

  useEffect(() => {
    setPage(1);
  }, [filter, sortBy, sortDirection]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const currentPage = Math.min(page, totalPages);
  const pagedRows = sorted.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  const sortIcon = (column) => {
    if (sortBy !== column) return "↕";
    return sortDirection === "asc" ? "↑" : "↓";
  };

  return (
    <div className="single-page">
      <section className="section-card">
        <div className="transactions-toolbar">
          <button type="button" className="secondary-btn" onClick={onBack}>
            Back to Dashboard
          </button>
          <div className="table-info">
            Showing {(currentPage - 1) * pageSize + (pagedRows.length > 0 ? 1 : 0)}-
            {(currentPage - 1) * pageSize + pagedRows.length} of {sorted.length}
          </div>
        </div>
        <div className="section-header">
          <h2>Reported Activity</h2>
          <div className="chip-group">
            {["ALL", "LOAN_PAYMENT", "BALANCE_SNAPSHOT"].map((option) => (
              <button
                key={option}
                type="button"
                className={filter === option ? "chip active" : "chip"}
                onClick={() => setFilter(option)}
              >
                {option === "ALL" ? "All" : option === "LOAN_PAYMENT" ? "Loan Payments" : "Balance Snapshots"}
              </button>
            ))}
          </div>
        </div>
        <div className="overview-grid">
          <article className="overview-card">
            <span>Total reported events</span>
            <strong>{summary.total_transactions || 0}</strong>
          </article>
          <article className="overview-card">
            <span>Total payment amount</span>
            <strong>{formatCurrency(summary.total_payment_amount)}</strong>
          </article>
          <article className="overview-card">
            <span>Late or past-due events</span>
            <strong>{summary.late_or_past_due_events || 0}</strong>
          </article>
          <article className="overview-card">
            <span>Reported utilization</span>
            <strong>{formatPercent(metrics.credit_utilization, 0)}</strong>
          </article>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>
                  <button type="button" className="sort-btn" onClick={() => toggleSort("transaction_date")}>
                    Date {sortIcon("transaction_date")}
                  </button>
                </th>
                <th>
                  <button type="button" className="sort-btn" onClick={() => toggleSort("activity")}>
                    Activity {sortIcon("activity")}
                  </button>
                </th>
                <th>
                  <button type="button" className="sort-btn" onClick={() => toggleSort("amount")}>
                    Amount {sortIcon("amount")}
                  </button>
                </th>
                <th>
                  <button type="button" className="sort-btn" onClick={() => toggleSort("expected_amount")}>
                    Reported Limit {sortIcon("expected_amount")}
                  </button>
                </th>
                <th>
                  <button type="button" className="sort-btn" onClick={() => toggleSort("status")}>
                    Status {sortIcon("status")}
                  </button>
                </th>
                <th>
                  Details
                </th>
              </tr>
            </thead>
            <tbody>
              {pagedRows.map((tx) => {
                const normalized = normalizeTransactionEvent(tx);
                return (
                  <tr key={tx.transaction_id}>
                    <td>{formatDate(tx.transaction_date)}</td>
                    <td>{normalized.label}</td>
                    <td>{formatCurrency(tx.amount)}</td>
                    <td>{formatCurrency(tx.expected_amount)}</td>
                    <td>
                      <span className={`status-pill ${normalized.tone}`}>{normalized.tone}</span>
                    </td>
                    <td>{transactionDetail(tx)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="pagination">
          <button
            type="button"
            className="pagination-btn"
            onClick={() => setPage((prev) => Math.max(1, prev - 1))}
            disabled={currentPage <= 1}
          >
            Previous
          </button>
          <span>
            Page {currentPage} of {totalPages}
          </span>
          <button
            type="button"
            className="pagination-btn"
            onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
            disabled={currentPage >= totalPages}
          >
            Next
          </button>
        </div>
      </section>
    </div>
  );
}

function CreditProfilePage({ dashboard }) {
  const profile = dashboard.profile;
  const metrics = dashboard.metrics || {};
  const summary = dashboard.transaction_summary || {};
  const scoreFactors = dashboard.factors || [];
  const paymentHealth = 1 - Number(metrics.late_payment_ratio || 0);

  return (
    <div className="single-page">
      <section className="section-card">
        <div className="section-header">
          <h2>Credit Profile</h2>
        </div>
        <p className="section-helper">
          Consumer-style summary of the account, payment, and utilization data currently reported into the platform.
        </p>
        <div className="profile-grid">
          <article className="profile-card">
            <span>Reported accounts</span>
            <strong>{Number(metrics.total_accounts || 0).toLocaleString()}</strong>
            <p>
              {Number(metrics.active_loans || 0).toLocaleString()} active loans and {Number(metrics.closed_loans || 0).toLocaleString()} closed loans
            </p>
          </article>
          <article className="profile-card">
            <span>Credit history length</span>
            <strong>{formatAgeLabel(metrics.account_age_days)}</strong>
            <p>Based on the oldest reported balance activity in your aggregated profile.</p>
          </article>
          <article className="profile-card">
            <span>Payment history</span>
            <strong>{formatPercent(paymentHealth, 0)}</strong>
            <p>
              {Number(metrics.late_payments || 0).toLocaleString()} late payments across {Number(metrics.total_payments || 0).toLocaleString()} reported payments
            </p>
          </article>
          <article className="profile-card">
            <span>Credit utilization</span>
            <strong>{formatPercent(metrics.credit_utilization, 0)}</strong>
            <p>{formatCurrency(metrics.avg_balance)} average reported balance.</p>
          </article>
        </div>
      </section>

      <section className="section-card">
        <div className="section-header">
          <h2>Accounts And Usage</h2>
        </div>
        <dl className="detail-grid">
          <dt>Active loans</dt>
          <dd>{Number(metrics.active_loans || 0).toLocaleString()}</dd>
          <dt>Closed loans</dt>
          <dd>{Number(metrics.closed_loans || 0).toLocaleString()}</dd>
          <dt>Total debt</dt>
          <dd>{formatCurrency(metrics.total_debt)}</dd>
          <dt>Debt-to-income ratio</dt>
          <dd>{formatPercent(metrics.debt_to_income_ratio, 0)}</dd>
          <dt>Highest reported balance</dt>
          <dd>{formatCurrency(metrics.max_balance)}</dd>
          <dt>Latest reported activity</dt>
          <dd>{formatDate(summary.latest_transaction_date)}</dd>
          <dt>Reporting institution</dt>
          <dd>{profile.institution_id || "-"}</dd>
          <dt>Source system</dt>
          <dd>{profile.source_system || "-"}</dd>
        </dl>
      </section>

      <section className="section-card">
        <div className="section-header">
          <h2>What Affects Your Score</h2>
        </div>
        <div className="key-factor-list">
          {scoreFactors.map((item, idx) => (
            <div key={`${item.factor}-${idx}`} className={`key-factor-item ${item.impact}`}>
              <span className="impact-icon">{item.impact === "negative" ? "↓" : item.impact === "positive" ? "↑" : "•"}</span>
              <div className="key-factor-copy">
                <strong>{item.factor}</strong>
                <span>{item.detail}</span>
                <small>Healthy benchmark: {item.benchmark}</small>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function SimulatorPage({ token, simulatorStatus, onSimulationApplied }) {
  const today = new Date().toISOString().slice(0, 10);
  const [form, setForm] = useState({
    transaction_type: "LOAN_PAYMENT",
    amount: "",
    expected_amount: "",
    days_past_due: "0",
    status: "ON_TIME",
    reference_id: "",
    transaction_date: today,
    raw_attributes_json: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  useEffect(() => {
    setForm((prev) => {
      const statusOptions =
        prev.transaction_type === "LOAN_PAYMENT" ? ["ON_TIME", "LATE", "PAST_DUE"] : ["CURRENT", "PAST_DUE"];
      const nextStatus = statusOptions.includes(prev.status) ? prev.status : statusOptions[0];
      return { ...prev, status: nextStatus };
    });
  }, [form.transaction_type]);

  const statusOptions =
    form.transaction_type === "LOAN_PAYMENT" ? ["ON_TIME", "LATE", "PAST_DUE"] : ["CURRENT", "PAST_DUE"];
  const quickAmounts = form.transaction_type === "LOAN_PAYMENT" ? [65, 120, 245, 480] : [150, 320, 650, 1200];
  const walletTitle = form.transaction_type === "LOAN_PAYMENT" ? "Loan repayment" : "Balance refresh";
  const walletSubtitle =
    form.transaction_type === "LOAN_PAYMENT"
      ? "Record an M-Pesa style repayment and rescore instantly."
      : "Capture a fresh wallet balance snapshot and update utilization.";
  const resultTone =
    Number(result?.score_change || 0) > 0 ? "positive" : Number(result?.score_change || 0) < 0 ? "negative" : "neutral";
  const phoneEvent = normalizeTransactionEvent({
    transaction_type: form.transaction_type,
    status: form.status,
    amount: Number(form.amount || 0),
    expected_amount: Number(form.expected_amount || 0),
    days_past_due: Number(form.days_past_due || 0),
  });

  const setField = (field, value) => setForm((prev) => ({ ...prev, [field]: value }));

  const submitSimulation = async (event) => {
    event.preventDefault();
    setError("");
    setResult(null);

    let rawAttributes = null;
    if (form.raw_attributes_json.trim()) {
      try {
        rawAttributes = JSON.parse(form.raw_attributes_json);
      } catch (parseError) {
        setError("Raw attributes must be valid JSON.");
        return;
      }
    }

    const amount = Number(form.amount);
    const expectedAmount = form.expected_amount.trim() ? Number(form.expected_amount) : null;
    const daysPastDue = Number(form.days_past_due || "0");
    if (!Number.isFinite(amount)) {
      setError("Amount is required and must be numeric.");
      return;
    }
    if (expectedAmount !== null && !Number.isFinite(expectedAmount)) {
      setError("Expected amount must be numeric.");
      return;
    }
    if (!Number.isFinite(daysPastDue) || daysPastDue < 0) {
      setError("Days past due must be zero or positive.");
      return;
    }

    const payload = {
      transaction_type: form.transaction_type,
      amount,
      expected_amount: expectedAmount,
      days_past_due: Math.round(daysPastDue),
      status: form.status,
      reference_id: form.reference_id.trim() || null,
      transaction_date: form.transaction_date || null,
      raw_attributes: rawAttributes,
    };

    setSubmitting(true);
    try {
      const response = await apiRequest("/api/me/simulator/transaction", token, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setResult(response);
      await onSimulationApplied();
    } catch (submitError) {
      setError(submitError.message || "Failed to submit simulation");
    } finally {
      setSubmitting(false);
    }
  };

  if (!simulatorStatus?.enabled) {
    return (
      <div className="single-page">
        <section className="section-card">
          <h2>Realtime Simulator</h2>
          <p className="section-helper">
            {simulatorStatus?.reason ||
              "Simulator is disabled. Configure OPERATIONAL_DB_URL and restart backend in operational DB mode."}
          </p>
        </section>
      </div>
    );
  }

  return (
    <div className="single-page">
      <section className="simulator-showcase">
        <div className="simulator-copy">
          <span className="simulator-eyebrow">Realtime Inference Playground</span>
          <h2>Mobile money simulator</h2>
          <p className="section-helper simulator-helper">
            The simulator now behaves like a customer wallet app: submit a mobile money payment, store the event in
            PostgreSQL, recompute online features, and score the customer with the deployed model.
          </p>

          <div className="simulator-kpis">
            <div className="simulator-kpi">
              <span>Flow</span>
              <strong>{"Bronze -> Silver -> Online Store"}</strong>
            </div>
            <div className="simulator-kpi">
              <span>Scoring</span>
              <strong>Deployed calibrated model</strong>
            </div>
            <div className="simulator-kpi">
              <span>Persistence</span>
              <strong>PostgreSQL score history</strong>
            </div>
          </div>

          <div className="simulator-side-card">
            <h3>What this screen controls</h3>
            <ul className="simulator-side-list">
              <li>Creates a raw mobile money event</li>
              <li>Normalizes it into the canonical transaction layer</li>
              <li>Updates online features for the customer</li>
              <li>Writes a new current score and history point</li>
            </ul>
          </div>

          {result ? (
            <div className={`simulator-result-panel ${resultTone}`}>
              <div className="simulator-result-top">
                <span className="simulator-result-chip">Last simulation</span>
                <strong>{formatSignedNumber(result.score_change)} pts</strong>
              </div>
              <div className="simulator-result-metrics">
                <div>
                  <span>Previous</span>
                  <strong>{result.previous_credit_score}</strong>
                </div>
                <div>
                  <span>Updated</span>
                  <strong>{result.new_credit_score}</strong>
                </div>
                <div>
                  <span>Risk</span>
                  <strong>{result.risk_level}</strong>
                </div>
              </div>
              <dl className="simulator-result-grid">
                <dt>Simulation ID</dt>
                <dd>{result.simulation_id}</dd>
                <dt>Transaction ID</dt>
                <dd>{result.transaction_id}</dd>
                <dt>Probability</dt>
                <dd>{(Number(result.new_default_probability || 0) * 100).toFixed(2)}%</dd>
                <dt>Ingested</dt>
                <dd>{formatDate(result.ingested_at)}</dd>
              </dl>
            </div>
          ) : (
            <div className="simulator-side-card muted">
              <h3>Ready to simulate</h3>
              <p className="section-helper">Submit from the phone mockup to generate a fresh transaction and score update.</p>
            </div>
          )}
        </div>

        <div className="phone-stage">
          <div className="phone-glow" />
          <div className="iphone-frame">
            <div className="iphone-notch" />
            <div className="iphone-screen">
              <div className="money-app">
                <div className="money-statusbar">
                  <span>9:41</span>
                  <span>5G</span>
                </div>

                <div className="money-app-header">
                  <div>
                    <p className="money-app-kicker">MobiCash Wallet</p>
                    <h3>{walletTitle}</h3>
                  </div>
                  <div className="money-avatar">MM</div>
                </div>

                <div className="money-balance-card">
                  <span>Available wallet</span>
                  <strong>{formatCurrency(4820)}</strong>
                  <p>{walletSubtitle}</p>
                </div>

                <div className="money-quick-actions">
                  <button type="button" className="money-chip active">
                    Pay loan
                  </button>
                  <button type="button" className="money-chip">
                    Buy airtime
                  </button>
                  <button type="button" className="money-chip">
                    Cash out
                  </button>
                </div>

                <form className="money-form" onSubmit={submitSimulation}>
                  <div className="money-segment">
                    {[
                      { value: "LOAN_PAYMENT", label: "Loan payment" },
                      { value: "BALANCE_SNAPSHOT", label: "Balance" },
                    ].map((option) => (
                      <button
                        key={option.value}
                        type="button"
                        className={form.transaction_type === option.value ? "money-segment-btn active" : "money-segment-btn"}
                        onClick={() => setField("transaction_type", option.value)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>

                  <label className="money-field money-field-amount">
                    <span>Amount</span>
                    <div className="money-amount-input">
                      <span>USD</span>
                      <input
                        type="number"
                        step="0.01"
                        value={form.amount}
                        onChange={(event) => setField("amount", event.target.value)}
                        placeholder="0.00"
                        required
                      />
                    </div>
                  </label>

                  <div className="money-presets">
                    {quickAmounts.map((amount) => (
                      <button key={amount} type="button" className="money-preset" onClick={() => setField("amount", String(amount))}>
                        {formatCurrency(amount)}
                      </button>
                    ))}
                  </div>

                  <div className="money-field-grid">
                    <label className="money-field">
                      <span>Expected</span>
                      <input
                        type="number"
                        step="0.01"
                        value={form.expected_amount}
                        onChange={(event) => setField("expected_amount", event.target.value)}
                        placeholder={form.transaction_type === "LOAN_PAYMENT" ? "Scheduled amount" : "Credit limit"}
                      />
                    </label>

                    <label className="money-field">
                      <span>When</span>
                      <input
                        type="date"
                        value={form.transaction_date}
                        onChange={(event) => setField("transaction_date", event.target.value)}
                      />
                    </label>
                  </div>

                  <label className="money-field">
                    <span>Reference</span>
                    <input
                      type="text"
                      value={form.reference_id}
                      onChange={(event) => setField("reference_id", event.target.value)}
                      placeholder="Loan or wallet reference"
                    />
                  </label>

                  <label className="money-field">
                    <span>Days past due</span>
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={form.days_past_due}
                      onChange={(event) => setField("days_past_due", event.target.value)}
                    />
                  </label>

                  <div className="money-status-group">
                    <span className="money-status-label">Status</span>
                    <div className="money-status-pills">
                      {statusOptions.map((statusOption) => (
                        <button
                          key={statusOption}
                          type="button"
                          className={form.status === statusOption ? "money-status-pill active" : "money-status-pill"}
                          onClick={() => setField("status", statusOption)}
                        >
                          {statusOption.replace("_", " ")}
                        </button>
                      ))}
                    </div>
                  </div>

                  <label className="money-field">
                    <span>Bronze override JSON</span>
                    <textarea
                      rows={3}
                      value={form.raw_attributes_json}
                      onChange={(event) => setField("raw_attributes_json", event.target.value)}
                      placeholder='{"channel":"agent"}'
                    />
                  </label>

                  {error ? <p className="error-text money-error">{error}</p> : null}

                  <button type="submit" className="money-submit-btn" disabled={submitting}>
                    {submitting ? "Processing payment..." : `Submit ${walletTitle}`}
                  </button>
                </form>

                <div className="money-feed">
                  <div className="money-feed-header">
                    <span>Realtime preview</span>
                    <strong>{phoneEvent.label}</strong>
                  </div>
                  <div className={`money-feed-card ${phoneEvent.tone}`}>
                    <div className="money-feed-icon">{phoneEvent.icon}</div>
                    <div>
                      <strong>{phoneEvent.detail}</strong>
                      <p>
                        {form.reference_id?.trim() || "Auto reference"} · {form.status.replace("_", " ")} ·{" "}
                        {form.amount ? formatCurrency(Number(form.amount)) : formatCurrency(0)}
                      </p>
                    </div>
                  </div>

                  {result ? (
                    <div className={`money-score-toast ${resultTone}`}>
                      <span>Score update</span>
                      <strong>
                        {result.previous_credit_score} → {result.new_credit_score}
                      </strong>
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export function App() {
  const navigate = useNavigate();
  const [token, setTokenState] = useState(() => getToken());
  const [authUser, setAuthUser] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [authBusy, setAuthBusy] = useState(false);
  const [error, setError] = useState("");

  const logout = () => {
    clearToken();
    setTokenState(null);
    setAuthUser(null);
    setData(null);
    setError("");
    navigate("/");
  };

  const loadPortalData = async (accessToken) => {
    const [me, dashboard, transactions, history, simulatorStatus] = await Promise.all([
      apiRequest("/api/auth/me", accessToken),
      apiRequest("/api/me/dashboard", accessToken),
      apiRequest("/api/me/transactions?limit=250", accessToken),
      apiRequest("/api/me/score-history", accessToken),
      apiRequest("/api/me/simulator/status", accessToken),
    ]);
    return { me, dashboard, transactions, history, simulatorStatus };
  };

  const refreshPortalData = async (accessToken = token) => {
    if (!accessToken) return;
    const payload = await loadPortalData(accessToken);
    setAuthUser(payload.me);
    setData(payload);
  };

  useEffect(() => {
    if (!token) return;
    let cancelled = false;

    async function bootstrap() {
      setLoading(true);
      try {
        const payload = await loadPortalData(token);
        if (cancelled) return;
        setAuthUser(payload.me);
        setData(payload);
        setError("");
      } catch (err) {
        if (cancelled) return;
        clearToken();
        setTokenState(null);
        setError(err.message || "Session expired. Please log in again.");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    bootstrap();
    return () => {
      cancelled = true;
    };
  }, [token]);

  const handleLogin = async (username, password) => {
    setAuthBusy(true);
    setError("");
    try {
      const response = await apiRequest("/api/auth/login", null, {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      setToken(response.access_token);
      setTokenState(response.access_token);
    } catch (err) {
      setError(err.message || "Unable to login");
    } finally {
      setAuthBusy(false);
    }
  };

  if (!token) return <LoginView onLogin={handleLogin} error={error} busy={authBusy} />;

  if (loading || !data || !authUser) {
    return (
      <div className="loading-screen">
        <div className="loading-card">
          <div className="spinner" />
          <p>Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <PortalLayout user={authUser} onLogout={logout}>
      <Routes>
        <Route
          path="/"
          element={
            <DashboardPage
              dashboard={data.dashboard}
              history={data.history}
              transactions={data.transactions}
              token={token}
              simulatorStatus={data.simulatorStatus}
              onViewAllTransactions={() => navigate("/transactions")}
              onViewCreditProfile={() => navigate("/credit-profile")}
            />
          }
        />
        <Route
          path="/transactions"
          element={<TransactionsPage transactions={data.transactions} dashboard={data.dashboard} onBack={() => navigate("/")} />}
        />
        <Route
          path="/simulator"
          element={
            <SimulatorPage
              token={token}
              simulatorStatus={data.simulatorStatus}
              onSimulationApplied={() => refreshPortalData(token)}
            />
          }
        />
        <Route path="/credit-profile" element={<CreditProfilePage dashboard={data.dashboard} />} />
        <Route path="/profile" element={<Navigate to="/credit-profile" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </PortalLayout>
  );
}
