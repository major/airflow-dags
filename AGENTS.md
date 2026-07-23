# airflow-dags

DAG bundle for a homehosted Apache Airflow deployment. Airflow's `GitDagBundle`
clones this repo directly and loads DAGs from `dags/`. There is no build,
test suite, or CI in this repo; a small lint/format/type-check toolchain
(ruff + pyright) runs locally via pre-commit.

## Deployment model

- Push to `main` — Airflow's DAG processor picks up changes on its next
  refresh automatically. No deploy step, no PR merge gate, no packaging.
- There is no local Airflow install — DAGs cannot be run locally. Correctness
  comes from writing valid Airflow code, reviewed by eye and gated by
  pre-commit (ruff + pyright).

## Local checks (pre-commit)

Every commit runs hooks defined in `.pre-commit-config.yaml`:

- `pre-commit-hooks` — trailing whitespace, EOF newline, YAML/TOML syntax,
  large-file guard, mixed line endings, etc.
- `ruff` (lint with `--fix`, then `ruff format`) — config in
  `pyproject.toml` under `[tool.ruff]`.
- `pyright` — config in `pyproject.toml` under `[tool.pyright]`, set to
  `typeCheckingMode = "basic"`.

One-time setup (host must have `pre-commit` available — e.g.
`pip install pre-commit` or `pipx install pre-commit`):

    pre-commit install

Run on demand without committing:

    pre-commit run --all-files

Run a single tool directly for faster iteration:

    ruff check --fix dags/
    ruff format dags/
    pyright dags/

### Why pyright doesn't fail on missing `airflow` imports

The pyright config sets `reportMissingImports = "none"` because the Airflow
SDK and provider packages are not installed in this repo's dev env — this
is a GitDagBundle, not a packaged project, and there is intentionally no
runtime install. Type checks run on what is resolvable (`requests`,
stdlib, etc.); the `airflow` types are left to be validated at runtime by
Airflow itself.

## Code conventions (from existing DAGs)

- Import from `airflow.sdk` (Airflow 3 TaskFlow API: `DAG`, `task`, `Param`,
  `BaseHook`, `get_current_context`), not the legacy `airflow.operators.*` /
  `airflow.models` style. See `dags/discord_test_message.py`.
- Every DAG file starts with `from __future__ import annotations` and a
  module docstring explaining what it does and any secret it needs.
- `schedule=None`, `catchup=False`, fixed `start_date=datetime.datetime(2024, 1, 1)`
  are the norm for these manually-triggered DAGs.

## Secrets

- **Never hardcode credentials in a DAG.** Secrets live outside this repo, in
  `~/git/major/homehosted` (SOPS-encrypted k8s Secret
  `apps/airflow/connections-secrets.sops.yaml`, name `airflow-connections`),
  injected into Airflow containers via `extraEnvFrom`.
- Each secret key is `AIRFLOW_CONN_<CONN_ID>`, a JSON connection string
  Airflow reads directly as a Connection (no `airflow connections add`).
- Pattern used by `discord_test_message`: DAG takes a `conn_id` Param
  (default `discord_webhook`), reads `BaseHook.get_connection(conn_id).password`
  at task run time. Adding a new target (e.g. another webhook) means adding a
  new `AIRFLOW_CONN_*` key in the homehosted repo and passing that `conn_id`
  when triggering — no code change here.
- To edit that secret: `sops apps/airflow/connections-secrets.sops.yaml` in
  the homehosted repo, then commit/push (Flux reconciles it).

## Keeping this file current

When a change substantially alters layout, conventions, secrets handling, or
deployment flow (e.g. adding tooling, CI, a package manifest, or a new DAG
pattern), update this `AGENTS.md` in the same change rather than letting it
go stale.

## The `massive` package

`dags/massive/` is the daily stock-market ETL pipeline that pulls OHLCV
from the [Massive.com](https://massive.com) REST API, applies corporate-split
adjustments, computes technical indicators, and runs data-quality spot
checks into a Postgres schema managed by these DAGs.

### Connection IDs

Two Airflow connections must exist (lives in the `airflow-connections`
k8s Secret, see the **Secrets** section above):

- `massive_api` — `password` field holds the Massive API key. Read at task
  runtime via `MassiveClient.from_env(conn_id="massive_api")`.
- `postgres_massive` — full DSN; the `massive` Postgres schema lives here.
  All six tables (`tickers`, `prices_raw`, `splits`, `prices_adjusted`,
  `indicators`, `spot_check_results`) are created on first run via the
  idempotent DDL block at the top of `sql.CREATE_TABLES_SQL`.

### Naming rule: forbidden legacy brand

Per the `massive-rs/AGENTS.md` guidance and the project's history, the
company's prior brand name is banned from this repo (the exact string is
not reproduced here; see `.slim/deepwork/massive-etl.md` and the
`massive-rs/AGENTS.md` for the canonical banned word). Use `massive`
everywhere in code, comments, docstrings, and DAG IDs. The Massive API
base URL is `https://api.massive.com`.

### Package layout

```text
dags/massive/
├── __init__.py        # empty package marker (Airflow loads each .py; pkg is namespacing)
├── sql.py             # DDL for 6 tables, apply-splits SQL, bulk_upsert_sql builder
├── db.py              # Postgres helpers (get_pg_conn, bulk_upsert, execute_script)
├── client.py          # MassiveClient: 4 endpoint methods, rate-limited, retry/backoff
├── spot_check.py      # build_spot_check_group factory: 6 delisted-aware DQ checks
├── dag_universe.py    # massive_refresh_universe (weekly Sun 02:00 ET)
├── dag_daily_etl.py   # massive_daily_etl (Mon-Fri 17:00 ET, 5-task chain)
└── dag_backfill.py    # massive_backfill (manual, OHLCV-only)
```

### DAGs and trigger ordering

The three DAGs have a strict manual-trigger ordering for first run:

1. **`massive_backfill`** (manual, OHLCV only) — populates `prices_raw`
   for a date range. Triggers: `from_date` and `to_date` Params; 5-year
   ceiling enforced. Dry-run mode (`dry_run=True` Param) sizes the run
   without writing.
2. **`massive_refresh_universe`** (auto, weekly Sun 02:00 ET) — runs
   automatically after backfill. Stamps `last_bar_date` and
   `first_bar_date` on each ticker. Will fire on the next Sun after
   backfill completes.
3. **`massive_daily_etl`** (auto, Mon-Fri 17:00 ET) — runs automatically.
   For the very first run after backfill, it will rebuild `prices_adjusted`
   and `indicators` from the full backfilled `prices_raw` history. The
   pandas-based `compute_indicators` task reads the entire
   `prices_adjusted` table; the first run after a 5-year backfill is
   the heaviest and may need `chunk_by_exchange=True` (a reserved Param
   not yet implemented).

All three DAGs are idempotent — re-running any of them is safe and
re-converges to the same state.

### Stock lifecycle policy

- **Delisted** tickers (no longer in `list_tickers?active=true`) are
  soft-deleted in `massive.tickers` (`active=false`, `delisted_date =
  last_bar_date`, `delisted_reason='fell_out_of_universe'`) once their
  most recent bar is older than 30 days. **Never delete**; historical
  bars stay forever so the dataset is survivorship-bias-free for
  backtesting.
- **Halted** tickers (still `active=true` but missing bars) are not
  flagged as delisted. The `freshness` spot-check warns at 6-10
  trading days of absence and fails at >10.
- **Renamed / symbol changes**: out of scope for the MVP. Bars under
  an old symbol stay attributed to that symbol forever; if a company
  is acquired and the acquirer reverse-lists under the old symbol, the
  new bars are a separate entity. Adding `ticker_events` tracking is a
  follow-up ticket.

### Backfill scope limitation (MVP)

`massive_backfill` pulls the **current** active universe only. Stocks
that were delisted during the 5-year backfill window are not included
in the historical load. This is intentional for the MVP (faster,
simpler); a survivorship-bias-free as-of-date backfill is a follow-up
ticket. Do not interpret the backfilled dataset as a complete market
history — it is a "what would the now-listed tickers' history look
like" dataset.

### Indicator conventions

Computed in `compute_indicators` (`dag_daily_etl.py`) on
`massive.prices_adjusted`:

- SMA via `pandas.rolling(window).mean()`; N ∈ {20, 50, 200}
- EMA via `pandas.ewm(span=N, adjust=False).mean()`; N ∈ {20, 50, 200}
- RSI(14) via Wilder's smoothing (`alpha=1/14`); first 14 rows per
  ticker are `NaN`
- MACD(12, 26, 9) via EMA differences; `macd_line = ema_12 - ema_26`,
  `macd_signal = ema_9(macd_line)`, `macd_hist = macd_line - macd_signal`

Price columns are `numeric(20,8)` to preserve penny-stock precision;
volume and transactions are `bigint`.

### Dependencies

- `requests` (already in the Airflow image)
- `psycopg2` (already in the Airflow image)
- `pandas` (assumed available; verify in the homehosted image)
- `exchange_calendars` (NYSECalendar) — used by `dag_backfill.py` and
  `spot_check.py` for trading-day window math. **Gracefully degrades**
  to a Mon-Fri filter with a one-line log warning if not installed;
  the DAGs and spot-checks still run, just with coarser holiday
  detection. Add to the Airflow image's `Pipfile`/`requirements.txt`
  in the homehosted repo for correct half-day and holiday handling.

### Spot-check Params

`massive_daily_etl` accepts these Params (defaulted; rarely overridden):

- `split_factor_mode` — `'auto'` (default), `'numerator'`, or
  `'reciprocal'`. Controls the direction of the
  `historical_adjustment_factor` cross-check in `split_factor_assertion`.
  The Massive API's direction was not pre-verified; `auto` accepts
  either direction and only flags when neither matches. After
  verifying with a single API call, set to `'numerator'` or
  `'reciprocal'` to lock the direction.
- `chunk_by_exchange` — `False` (default). Reserved for chunking the
  `compute_indicators` SELECT by `primary_exchange` to reduce memory
  pressure on a 5-year-backfilled DB. Not yet implemented.

### No Discord integration

The original plan included a `post_discord` task in the daily ETL
chain. It was removed. **Airflow's built-in success/failure alerting
is the source of truth** for run state. Discord posting is not part
of this package; do not add it.

### Operational notes

- `massive_refresh_universe` is the only writer of `active`,
  `delisted_date`, `last_bar_date`, and `first_bar_date` on
  `massive.tickers`. No other DAG should modify those columns.
- `massive_daily_etl.ingest_splits` catches `CheckViolation` per row
  (the `massive_splits_positive` CHECK rejects `split_from=0` or
  `split_to=0`) and logs+skips the offending event rather than failing
  the whole upsert.
- The `apply_splits` task sets `statement_timeout='15min'` for the
  rebuild transaction. On a 5-year-backfilled DB the TRUNCATE+INSERT
  rebuild of `prices_adjusted` is the heaviest single statement in
  the pipeline.
- The `prices_raw` table does not have a standalone `(bar_date)`
  index; the composite PK `(ticker, bar_date)` covers per-ticker
  freshness queries. If a future "latest bar in the table" query
  becomes slow, add the standalone index.

The deepwork file for this package is at
`.slim/deepwork/massive-etl.md` (git-local, OpenCode-readable). It
contains the full locked-decision log, the Massive API research notes,
and the Phase 1 / Phase 2 Oracle review reconciliation.
