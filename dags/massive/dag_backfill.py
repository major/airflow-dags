"""
One-shot historical backfill of unadjusted OHLCV from Massive for all US common stocks + ETFs.

Triggered manually with a ``from_date`` and ``to_date`` Param; iterates trading
days (Mon-Fri minus US market holidays) and writes a daily market-summary pull
to ``massive.prices_raw`` per day.

This DAG is **OHLCV only** — splits, adjusted prices, and indicators are NOT
computed here. After this DAG completes, the operator must manually trigger
``massive_daily_etl`` once (which will pick up the late-arriving splits and
compute adjusted prices + indicators for the backfilled range).

**Rate limiting:** Massive's "Stocks Starter" tier allows <100 requests per
second. The backfill makes one API call per trading day (~1,260 for 5 years),
totaling ~1,260 calls over the 3-hour execution window. A 50ms sleep between
days plus the client's built-in exponential backoff on 429/5xx keeps us well
under the limit.
"""

from __future__ import annotations

import datetime
import logging
import time

import psycopg2

from airflow.sdk import DAG, Param, get_current_context, task
from massive import sql
from massive.client import MassiveClient
from massive.db import bulk_upsert, execute_script, get_pg_conn

logger = logging.getLogger(__name__)

_MAX_BACKFILL_YEARS = 5
_EARLIEST_MASSIVE_DATE = datetime.date(2003, 9, 10)
# weekday() returns 0=Monday ... 6=Sunday; < 5 means Mon-Fri
_SATURDAY_INDEX = 5

# ---- Exchange calendar ------------------------------------------------

try:
    import exchange_calendars as _ec

    _HAS_EXCHANGE_CALENDARS = True
except ImportError:
    _HAS_EXCHANGE_CALENDARS = False
    _ec = None  # type: ignore[assignment]
    logger.warning(
        "exchange_calendars not available; falling back to Mon-Fri filter "
        "for trading days. Market holidays will not be excluded."
    )


def _get_trading_days(
    from_date: datetime.date,
    to_date: datetime.date,
) -> list[datetime.date]:
    """Return all US trading days (NYSE calendar) in [from_date, to_date]."""
    if _HAS_EXCHANGE_CALENDARS:
        # _ec is guaranteed to be the exchange_calendars module here
        # because _HAS_EXCHANGE_CALENDARS is only True after a successful import.
        cal = _ec.get_calendar("XNYS")  # type: ignore[union-attr]
        sessions = cal.sessions_in_range(from_date, to_date)
        return [s.date() for s in sessions]
    # Fallback: Mon-Fri filter (does not exclude market holidays).
    days: list[datetime.date] = []
    current = from_date
    while current <= to_date:
        if current.weekday() < _SATURDAY_INDEX:
            days.append(current)
        current += datetime.timedelta(days=1)
    return days


# ---- Bootstrap task ---------------------------------------------------


@task
def bootstrap() -> dict:
    """
    Validate params, bootstrap DB schema (if not dry run), return day count.

    Raises
    ------
    ValueError
        When validation fails (date range too large, out of bounds, etc.).

    """
    ctx = get_current_context()
    params = ctx["params"]

    from_raw: str = params["from_date"]
    to_raw: str = params["to_date"]
    include_otc: bool = params["include_otc"]
    dry_run: bool = params["dry_run"]

    from_date = datetime.date.fromisoformat(from_raw)
    to_date = datetime.date.fromisoformat(to_raw)

    # -- Validation --
    if from_date > to_date:
        msg = f"from_date ({from_raw}) must not be after to_date ({to_raw})."
        raise ValueError(msg)
    if (to_date - from_date) > datetime.timedelta(days=365 * _MAX_BACKFILL_YEARS):
        msg = (
            f"Backfill range exceeds {_MAX_BACKFILL_YEARS} years "
            f"({from_raw} → {to_raw}). Reduce the window or run multiple backfills."
        )
        raise ValueError(msg)
    if from_date < _EARLIEST_MASSIVE_DATE:
        msg = (
            f"Massive market-summary data only goes back to "
            f"{_EARLIEST_MASSIVE_DATE.isoformat()}. Requested from_date={from_raw}."
        )
        raise ValueError(msg)

    trading_days = _get_trading_days(from_date, to_date)

    if dry_run:
        # Estimate universe size from the first day's market summary.
        universe_size = 0
        try:
            client = MassiveClient.from_env()
            universe_size = len(
                client.get_market_summary(from_date, include_otc=include_otc, adjusted=False)
            )
        except Exception:
            logger.warning("Could not estimate universe size for dry run.", exc_info=True)

        logger.info(
            "[DRY RUN] would backfill %d trading days from %s to %s; "
            "universe size %d (current ETF+CS count). Returning.",
            len(trading_days),
            from_raw,
            to_raw,
            universe_size,
        )
        return {
            "trading_days": len(trading_days),
            "from_date": from_raw,
            "to_date": to_raw,
            "dry_run": True,
        }

    # Bootstrap DDL (idempotent).
    conn = get_pg_conn()
    try:
        execute_script(conn, sql.CREATE_TABLES_SQL)
        logger.info("Schema bootstrapped via CREATE_TABLES_SQL.")
    finally:
        conn.close()

    return {
        "trading_days": len(trading_days),
        "from_date": from_raw,
        "to_date": to_raw,
        "dry_run": False,
    }


# ---- Per-day helper for backfill_range --------------------------------


def _process_trading_day(
    day: datetime.date,
    columns: list[str],
    conn: psycopg2.extensions.connection,
    *,
    include_otc: bool,
) -> tuple[int, int] | None:
    """
    Fetch *day*|s market summary and upsert to ``prices_raw``.

    Parameters
    ----------
    day:
        Trading date to process.
    columns:
        Column names for the upsert.
    conn:
        Open Postgres connection (a single connection is reused across days).
    include_otc:
        Whether to include OTC-traded tickers.

    Returns
    -------
    ``(rows_written, rows_skipped_invalid)`` if the day was processed
    (including UniqueViolation days where 0 rows were written), or ``None``
    when the API returned no bars (holiday / non-trading day).

    Raises
    ------
    MassiveAPIError
        When the API request fails (hard failure; operator must investigate).

    """
    client = MassiveClient.from_env()
    try:
        bars = client.get_market_summary(day, include_otc=include_otc, adjusted=False)
    except Exception:
        logger.exception("Failed to fetch market summary for %s. Aborting backfill.", day)
        raise

    if not bars:
        logger.info("No market data for %s; likely a holiday.", day)
        return None

    # Secondary filter (belt-and-suspenders; client already filters
    # internally via the same check during get_market_summary).
    valid_bars: list = []
    rows_skipped = 0
    for bar in bars:
        ok, reason = MassiveClient._is_valid_bar(bar)  # noqa: SLF001
        if ok:
            valid_bars.append(bar)
        else:
            rows_skipped += 1
            logger.debug("Skipped invalid bar %s/%s: %s", bar.ticker, day, reason)

    ingested_at = datetime.datetime.now(tz=datetime.UTC)
    rows = [
        (bar.ticker, day, bar.o, bar.h, bar.l, bar.c, bar.v, bar.vw, bar.n, ingested_at)
        for bar in valid_bars
    ]

    try:
        bulk_upsert(conn, "prices_raw", columns, rows, conflict_keys=["ticker", "bar_date"])
    except psycopg2.errors.UniqueViolation:
        logger.warning("UniqueViolation on %s; continuing (defense in depth).", day)
        conn.rollback()
        return (0, rows_skipped)

    logger.info("%s: wrote %d bars (%d invalid skipped)", day, len(rows), rows_skipped)
    return (len(rows), rows_skipped)


# ---- Backfill-range task ----------------------------------------------


@task
def backfill_range(prior: dict) -> dict:
    """
    Iterate trading days, pull market summary, upsert to prices_raw.

    Parameters
    ----------
    prior:
        XCom output from :func:`bootstrap` (used for dry-run signal and
        informational logging).

    """
    ctx = get_current_context()
    params = ctx["params"]

    from_raw: str = params["from_date"]
    to_raw: str = params["to_date"]
    include_otc: bool = params["include_otc"]
    dry_run: bool = params["dry_run"]

    if dry_run:
        logger.info("Dry-run mode; no data written. prior=%s", prior)
        return {
            "days_processed": 0,
            "days_skipped_empty": 0,
            "rows_written": 0,
            "rows_skipped_invalid": 0,
        }

    from_date = datetime.date.fromisoformat(from_raw)
    to_date = datetime.date.fromisoformat(to_raw)
    trading_days = _get_trading_days(from_date, to_date)

    columns = [
        "ticker",
        "bar_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "ingested_at",
    ]

    days_processed = 0
    days_skipped_empty = 0
    rows_written = 0
    rows_skipped_invalid = 0

    conn = get_pg_conn()
    try:
        for day in trading_days:
            result = _process_trading_day(day, columns, conn, include_otc=include_otc)
            if result is None:
                days_skipped_empty += 1
            else:
                written, skipped = result
                days_processed += 1
                rows_written += written
                rows_skipped_invalid += skipped

            # Throttle: 50ms between days (well under Massive's <100 req/s ceiling).
            time.sleep(0.05)
    finally:
        conn.close()

    return {
        "days_processed": days_processed,
        "days_skipped_empty": days_skipped_empty,
        "rows_written": rows_written,
        "rows_skipped_invalid": rows_skipped_invalid,
    }


# ---- DAG definition ---------------------------------------------------

with DAG(
    dag_id="massive_backfill",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["massive", "etl", "backfill", "manual"],
    default_args={
        "retries": 5,
        "retry_delay": datetime.timedelta(minutes=5),
        "execution_timeout": datetime.timedelta(hours=3),
    },
    params={
        "from_date": Param(
            "2021-01-01",
            type="string",
            format="date",
            title="Start date (inclusive)",
            description="Earliest trading day to backfill. Format YYYY-MM-DD.",
        ),
        "to_date": Param(
            "2025-12-31",
            type="string",
            format="date",
            title="End date (inclusive)",
            description="Latest trading day to backfill. Format YYYY-MM-DD.",
        ),
        "include_otc": Param(
            default=False,
            type="boolean",
            title="Include OTC",
            description=(
                "If true, include OTC tickers in the per-day market summary. Default false."
            ),
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
            title="Dry run",
            description=(
                "If true, log the planned daily calls and the universe size "
                "without writing to the database. Useful for sizing the run "
                "before committing."
            ),
        ),
    },
) as dag:
    backfill_range(bootstrap())
