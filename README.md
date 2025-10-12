# DashFolio

DashFolio is a hybrid Flask and TypeScript application that helps long-term investors
monitor portfolio health, understand risk exposure, and document cash activity in a
single, data-rich workspace. The backend orchestrates market-data ingestion, risk
analytics, and onboarding workflows, while the front-end delivers an interactive
experience for exploring holdings, allocations, and simulated risk scenarios.

## Key capabilities

- **Holistic portfolio overview** – track equity, ETF, and cash positions with live
  pricing updates, target allocations, and customizable benchmarks.
- **Risk and scenario analysis** – compute exponentially weighted moving average
  (EWMA) returns, trailing-stop hit probabilities, and Value-at-Risk (VaR) estimates
  so you can anticipate drawdowns before they happen.
- **Cash-flow awareness** – log deposits, withdrawals, dividends, and interest
  adjustments, and apply them to derived holdings in a normalized ledger.
- **Allocation sandboxing** – model hypothetical assets directly in the allocation
  planner and explore rebalancing actions without impacting live holdings.
- **Audit-friendly data trail** – persist every snapshot, calculation, and user event
  in an embedded SQLite database for reproducibility and historical comparisons.
- **Single-user secure onboarding** – password-protected authentication, guided
  funding steps, and CSV transaction ingestion for streamlined setup.

## Architecture at a glance

| Layer | Responsibilities | Representative modules |
| --- | --- | --- |
| **Presentation** | Flask templates render the onboarding, holdings, allocations, and risk dashboards; static CSS styles cards, charts, and modal dialogs. | `templates/`, `static/css/app.css` |
| **Application services** | Authentication, configuration, formatting, and portfolio state helpers wrap Flask routes with reusable business logic. | `services/auth.py`, `services/configuration.py`, `services/portfolio.py` |
| **Calculations engine** | Loads transactions, prices, and benchmarks, then runs analytics such as EWMA statistics, trailing-stop simulations, and snapshot caching. | `Calculations/` package |
| **Data layer** | An embedded SQLite database stores price history, derived holdings, risk simulations, and cached snapshots for fast recomputation. | `Calculations/storage.py`, `dashfolio.db` |

## Data sources

DashFolio consumes data from multiple sources:

- **Portfolio configuration** – base holdings, target allocations, and security metadata
  live in `portfolio.json` and are synchronized with transaction-derived holdings.
- **User preferences** – `config.json` defines the analysis window, stop-loss ranges,
  EWMA spans, benchmark tickers, and UI auto-refresh cadence.
- **Market data** – live quotes and historical candles are pulled from Yahoo Finance via
  [`yfinance`](https://pypi.org/project/yfinance/), then normalized and cached locally.
- **User transactions** – CSV uploads and manual adjustments are validated, normalized,
  and written into SQLite tables to produce reproducible holdings snapshots.

## Getting started

1. **Create a Python environment** (3.11+ recommended) and install the application
   dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. *(Optional)* **Choose a configuration directory** if you do not want
   DashFolio to store `config.json`, `portfolio.json`, and `dashfolio.db` in the
   project root:
   ```bash
   export DASHFOLIO_CONFIG_DIR="$HOME/.local/share/dashfolio"
   ```
3. **Provision the database** by running the calculation engine once:
   ```bash
   python main.py
   ```
   This fetches the initial market data, updates holdings prices, and seeds the
   risk-analysis tables.
4. **Launch the Flask application** and explore the dashboard:
   ```bash
   export FLASK_SECRET_KEY="replace-with-a-long-random-string"
   export FLASK_APP=app.py
   flask run --debug
   ```
   Visit `http://127.0.0.1:5000` to complete onboarding, upload transactions, and
   review portfolio analytics.
5. **Run the TypeScript unit tests** that cover the client-side allocation logic:
   ```bash
   npm install
   npm test
   ```

### Running with Docker

DashFolio ships with a multi-stage Dockerfile and Compose definition that mount a
durable configuration volume at `/config` inside the container. To build and run
the stack:

```bash
export FLASK_SECRET_KEY="replace-with-a-long-random-string"
docker compose up --build
```

The compose file binds `/mnt/nas-share/config/stocks` on the host to `/config`
in the container so your SQLite database and JSON files persist across restarts.
To skip the initial analytics bootstrap on start-up set `DASHFOLIO_SKIP_BOOTSTRAP=1`.

### Currency configuration

Set the `CURRENCY` field in `config.json` to any supported ISO code (`USD`,
`BHD`, `EUR`, `GBP`, `CAD`, etc.). Provide custom rates or symbols via the
`CURRENCY_RATE_OVERRIDES` mapping:

```json
{
  "CURRENCY": "EUR",
  "CURRENCY_RATE_OVERRIDES": {
    "EUR": {"rate": 0.92, "symbol": "€", "symbol_first": true}
  }
}
```

To fetch live USD exchange rates, export `DASHFOLIO_ENABLE_LIVE_FX=1`. You can
override the default API endpoint (`https://open.er-api.com/v6/latest/{base}`)
with `DASHFOLIO_FX_API_URL`.

## Dashboard walkthrough

1. **Onboarding & authentication** – register the primary user, define funding
   balances, and upload CSV transaction history. On success, onboarding updates the
   SQLite-backed portfolio tables and unlocks the analytics experience.
2. **Portfolio analysis** – inspect each holding’s live price, target vs. actual
   allocation, and recent performance metrics. Drill into holdings for transaction
   history, price series, and recommended rebalancing moves.
3. **Risk analysis** – experiment with trailing-stop ranges and confidence levels to
   understand the probability of stop activation and the projected loss magnitude at
   different VaR thresholds.
4. **Transactions** – review the normalized transaction ledger, including deposits,
   withdrawals, dividends, and interest adjustments that reconcile to the cash balance.
5. **Settings** – tune the analysis horizon, EWMA span, benchmark ticker, and session
   duration. Changes propagate immediately to the calculation engine and cached
   snapshots.

## Contributing

Pull requests are welcome! Please accompany changes with unit tests (`npm test` for the
TypeScript utilities and any relevant Python tests), and update documentation when you
extend the data model or dashboard workflows.

## License

This project is distributed under the MIT License. See `LICENSE` (if present) for the
full text.
