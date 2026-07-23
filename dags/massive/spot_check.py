"""
Six data-quality checks for the Massive ETL, exposed as an Airflow TaskGroup.

Checks
------
1. **freshness** — per active ticker, is the latest bar within 5 trading days?
2. **continuity** — per active ticker, are the last 20 bars gapless?
3. **survivorship_audit** — do ``active=false`` tickers match stale-bar tickers?
4. **split_factor_assertion** — does ``split_to / split_from`` match
   ``historical_adjustment_factor``?
5. **indicator_coverage** — does every active ticker have a computed indicator?
6. **summary** — aggregates per-check results into one ``daily_summary`` row.

All checks are delisted-aware: tickers past their ``delisted_date`` are skipped
rather than flagged as missing.
"""

from __future__ import annotations

import datetime
import json
import logging
from typing import TYPE_CHECKING, Any

from psycopg2.extras import execute_values

from airflow.sdk import DAG, TaskGroup, get_current_context, task
from massive.db import get_pg_conn

if TYPE_CHECKING:
    from psycopg2.extensions import connection as _connection
else:
    _connection = Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FRESHNESS_GAP_OK = 5
_FRESHNESS_GAP_WARN = 10
_CONTINUITY_MIN_BARS = 20
_CONTINUITY_WARN_THRESHOLD = 2
_SURVIVORSHIP_DIVERGE_PCT = 5.0
_FACTOR_EPSILON = 1e-10
_WEEKDAY_SATURDAY = 5

UTC = datetime.UTC

# ---------------------------------------------------------------------------
# Trading-day calendar
# ---------------------------------------------------------------------------

try:
    import exchange_calendars as _ec  # type: ignore[import-untyped]

    _HAS_EXCHANGE_CALENDARS = True
except ImportError:
    _HAS_EXCHANGE_CALENDARS = False
    _ec = None  # type: ignore[assignment]
    logger.warning(
        "exchange_calendars not available; spot-check trading-day "
        "calculations will use a simple Mon-Fri filter "
        "(US market holidays ignored)."
    )


def _trading_days_between(d1: datetime.date, d2: datetime.date) -> int:
    """Return number of NYSE trading days in the half-open interval ``(d1, d2]``."""
    if d1 >= d2:
        return 0
    if _HAS_EXCHANGE_CALENDARS:
        cal = _ec.get_calendar("XNYS")  # type: ignore[union-attr]
        sessions = cal.sessions_in_range(d1 + datetime.timedelta(days=1), d2)
        return len(sessions)
    # Mon-Fri fallback (does not exclude market holidays).
    count = 0
    d = d1 + datetime.timedelta(days=1)
    while d <= d2:
        if d.weekday() < _WEEKDAY_SATURDAY:
            count += 1
        d += datetime.timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_INSERT_RESULTS_SQL = """\
INSERT INTO massive.spot_check_results
    (run_id, check_name, ticker, status, detail, checked_at)
VALUES %s\
"""


def _batch_insert_results(
    conn: _connection,
    rows: list[tuple[Any, ...]],
) -> None:
    """
    Upsert rows into ``spot_check_results`` via ``execute_values``.

    Each row is ``(run_id, check_name, ticker, status, detail_json, checked_at)``
    where *detail_json* is a JSON-encoded ``str`` or ``None``.
    """
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            _INSERT_RESULTS_SQL,
            rows,
            template="(%s, %s, %s, %s, %s::jsonb, %s)",
        )
    conn.commit()


def _get_target_date(ctx: dict[str, Any]) -> datetime.date:
    """
    Extract the target trading date from the Airflow run context.

    For a 17:00 ET Mon-Fri schedule, ``data_interval_end`` is the current
    day at 17:00 ET (e.g., Tuesday 17:00 for the Tuesday run).  The target
    trading day is that day itself — the market just closed and data is now
    available.
    """
    try:
        data_end = ctx["data_interval_end"]
    except KeyError:
        data_end = ctx.get("logical_date", ctx.get("execution_date"))
    return data_end.date()  # type: ignore[union-attr]


def _run_id(ctx: dict[str, Any]) -> str:
    """Safely extract ``run_id`` from the run context."""
    rid = ctx.get("run_id")
    if rid is None:
        rid = "unknown"
    return rid


# ---------------------------------------------------------------------------
# Task implementations (module-level so Airflow can discover them)
# ---------------------------------------------------------------------------


@task
def freshness() -> dict[str, int]:
    """Check that every active ticker has a bar within 5 trading days."""
    ctx = get_current_context()
    dag_run_date = _get_target_date(ctx)
    run_id = _run_id(ctx)
    checked_at = datetime.datetime.now(tz=UTC)

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, delisted_date, last_bar_date "
                "FROM massive.tickers WHERE active = true",
            )
            ticker_rows = cur.fetchall()

        result: dict[str, int] = {
            "ok": 0,
            "warn": 0,
            "fail": 0,
            "skipped_delisted": 0,
        }
        failing_rows: list[tuple[Any, ...]] = []

        for ticker, delisted_date, last_bar_date in ticker_rows:  # type: ignore[union-attr]
            # Skip tickers that are past their delisted date.
            if (
                delisted_date is not None
                and last_bar_date is not None
                and delisted_date <= last_bar_date
            ):
                result["skipped_delisted"] += 1
                continue

            if last_bar_date is None:
                # No bars yet (backfill may not have run).
                result["ok"] += 1
                continue

            gap = _trading_days_between(last_bar_date, dag_run_date)

            if gap <= _FRESHNESS_GAP_OK:
                result["ok"] += 1
            elif gap <= _FRESHNESS_GAP_WARN:
                result["warn"] += 1
                failing_rows.append(
                    (
                        run_id,
                        "freshness",
                        ticker,
                        "warn",
                        json.dumps(
                            {
                                "gap_trading_days": gap,
                                "last_bar_date": last_bar_date.isoformat(),
                            }
                        ),
                        checked_at,
                    )
                )
            else:
                result["fail"] += 1
                failing_rows.append(
                    (
                        run_id,
                        "freshness",
                        ticker,
                        "fail",
                        json.dumps(
                            {
                                "gap_trading_days": gap,
                                "last_bar_date": last_bar_date.isoformat(),
                            }
                        ),
                        checked_at,
                    )
                )

        _batch_insert_results(conn, failing_rows)
        return result
    finally:
        conn.close()


@task
def continuity() -> dict[str, int]:
    """Check the last 20 bars of each active ticker for gaps."""
    ctx = get_current_context()
    dag_run_date = _get_target_date(ctx)
    run_id = _run_id(ctx)
    checked_at = datetime.datetime.now(tz=UTC)

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, delisted_date FROM massive.tickers WHERE active = true",
            )
            tickers_info = dict(cur.fetchall())

        # Only aggregate bars from the last ~45 calendar days (~30 trading
        # days) to avoid scanning the entire prices_raw table (~16M rows).
        cutoff = dag_run_date - datetime.timedelta(days=45)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, array_agg(bar_date ORDER BY bar_date DESC) "
                "FROM ("
                "  SELECT ticker, bar_date "
                "  FROM massive.prices_raw "
                "  WHERE bar_date >= %s"
                ") recent "
                "GROUP BY ticker",
                (cutoff,),
            )
            ticker_bars: dict[str, list[datetime.date]] = {}
            for ticker, dates in cur.fetchall():
                if len(dates) >= _CONTINUITY_MIN_BARS:
                    ticker_bars[ticker] = sorted(dates[:_CONTINUITY_MIN_BARS])

        result: dict[str, int] = {
            "ok": 0,
            "warn": 0,
            "fail": 0,
            "skipped_delisted": 0,
        }
        failing_rows: list[tuple[Any, ...]] = []

        for ticker, bars in ticker_bars.items():
            delisted_date = tickers_info.get(ticker)

            if delisted_date is not None and delisted_date <= bars[-1]:
                result["skipped_delisted"] += 1
                continue

            missing = 0
            for i in range(len(bars) - 1):
                gap = _trading_days_between(bars[i], bars[i + 1])
                if gap > 1:
                    missing += gap - 1

            if missing == 0:
                result["ok"] += 1
            elif missing <= _CONTINUITY_WARN_THRESHOLD:
                result["warn"] += 1
                failing_rows.append(
                    (
                        run_id,
                        "continuity",
                        ticker,
                        "warn",
                        json.dumps(
                            {
                                "missing_trading_days": missing,
                                "checked_range": (f"{bars[0].isoformat()}..{bars[-1].isoformat()}"),
                            }
                        ),
                        checked_at,
                    )
                )
            else:
                result["fail"] += 1
                failing_rows.append(
                    (
                        run_id,
                        "continuity",
                        ticker,
                        "fail",
                        json.dumps(
                            {
                                "missing_trading_days": missing,
                                "checked_range": (f"{bars[0].isoformat()}..{bars[-1].isoformat()}"),
                            }
                        ),
                        checked_at,
                    )
                )

        _batch_insert_results(conn, failing_rows)
        return result
    finally:
        conn.close()


@task
def survivorship_audit() -> dict[str, Any]:
    """
    Compare ``active=false`` count vs stale-bar ticker count.

    If the two diverge by more than 5 %, the universe refresh or data
    retention may be broken.
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM massive.tickers WHERE active = false",
            )
            row = cur.fetchone()
            inactive_count = row[0] if row is not None else 0

            cur.execute(
                "SELECT COUNT(DISTINCT ticker) "
                "FROM ("
                "  SELECT ticker, MAX(bar_date) AS last_bar "
                "  FROM massive.prices_raw GROUP BY ticker"
                ") t "
                "WHERE last_bar < CURRENT_DATE - INTERVAL '30 days'",
            )
            row = cur.fetchone()
            stale_count = row[0] if row is not None else 0

        if inactive_count == 0 and stale_count == 0:
            pct_diff = 0.0
        elif inactive_count == 0:
            pct_diff = 100.0
        else:
            pct_diff = abs(inactive_count - stale_count) / inactive_count * 100

        status = "fail" if pct_diff > _SURVIVORSHIP_DIVERGE_PCT else "ok"
        ctx = get_current_context()
        run_id = _run_id(ctx)

        _batch_insert_results(
            conn,
            [
                (
                    run_id,
                    "survivorship_audit",
                    None,
                    status,
                    json.dumps(
                        {
                            "inactive_count": inactive_count,
                            "stale_count": stale_count,
                            "pct_diff": round(pct_diff, 2),
                        }
                    ),
                    datetime.datetime.now(tz=UTC),
                )
            ],
        )

        return {
            "status": status,
            "inactive_count": inactive_count,
            "stale_count": stale_count,
            "pct_diff": round(pct_diff, 2),
        }
    finally:
        conn.close()


@task
def split_factor_assertion() -> dict[str, Any]:
    """
    Cross-check ``historical_adjustment_factor`` against computed ratio.

    Direction is controlled by the ``split_factor_mode`` DAG Param
    (``auto``, ``numerator``, or ``reciprocal``; default ``auto``).
    """
    ctx = get_current_context()
    run_id = _run_id(ctx)
    params = ctx.get("params", {})
    if params is None:
        params = {}
    mode = params.get("split_factor_mode", "auto")
    checked_at = datetime.datetime.now(tz=UTC)

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, split_from, split_to, "
                "       historical_adjustment_factor "
                "FROM massive.splits "
                "WHERE historical_adjustment_factor IS NOT NULL",
            )
            splits = cur.fetchall()

        mismatches = 0
        total = len(splits)
        failing_rows: list[tuple[Any, ...]] = []

        for ticker, sf, st, factor in splits:
            try:
                sf_f = float(sf)
                st_f = float(st)
                factor_f = float(factor)
            except (ValueError, TypeError):
                mismatches += 1
                continue

            if sf_f == 0:
                mismatches += 1
                continue

            expected = st_f / sf_f
            reciprocal = 1.0 / expected if expected != 0 else None

            if mode == "auto":
                ok = abs(expected - factor_f) < _FACTOR_EPSILON or (
                    reciprocal is not None and abs(reciprocal - factor_f) < _FACTOR_EPSILON
                )
            elif mode == "numerator":
                ok = abs(expected - factor_f) < _FACTOR_EPSILON
            elif mode == "reciprocal":
                ok = reciprocal is not None and abs(reciprocal - factor_f) < _FACTOR_EPSILON
            else:
                ok = False

            if not ok:
                mismatches += 1
                failing_rows.append(
                    (
                        run_id,
                        "split_factor_assertion",
                        ticker,
                        "fail",
                        json.dumps(
                            {
                                "split_from": sf_f,
                                "split_to": st_f,
                                "expected_numerator": expected,
                                "expected_reciprocal": reciprocal,
                                "actual_factor": factor_f,
                                "mode": mode,
                            }
                        ),
                        checked_at,
                    )
                )

        status = "fail" if mismatches > 0 else "ok"
        failing_rows.append(
            (
                run_id,
                "split_factor_assertion",
                None,
                status,
                json.dumps(
                    {
                        "total_checked": total,
                        "mismatches": mismatches,
                        "mode": mode,
                    }
                ),
                checked_at,
            )
        )
        _batch_insert_results(conn, failing_rows)

        return {
            "status": status,
            "total_checked": total,
            "mismatches": mismatches,
        }
    finally:
        conn.close()


@task
def indicator_coverage() -> dict[str, int]:
    """
    Check every active ticker's latest bar has indicators.

    Missing indicators are ``warn`` only — indicators are best-effort
    and not part of the DAG's critical path.
    """
    ctx = get_current_context()
    run_id = _run_id(ctx)
    checked_at = datetime.datetime.now(tz=UTC)

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT t.ticker, t.last_bar_date, i.bar_date AS indicator_bar_date "
                "FROM massive.tickers t "
                "LEFT JOIN massive.indicators i "
                "    ON i.ticker = t.ticker AND i.bar_date = t.last_bar_date "
                "WHERE t.active = true "
                "  AND (t.delisted_date IS NULL "
                "       OR t.delisted_date > t.last_bar_date "
                "       OR t.last_bar_date IS NULL) "
                "  AND t.last_bar_date IS NOT NULL",
            )
            rows = cur.fetchall()

        ok_count = 0
        warned = 0
        warn_rows: list[tuple[Any, ...]] = []

        for ticker, last_bar, indicator_bar_date in rows:
            if indicator_bar_date is not None:
                ok_count += 1
            else:
                warned += 1
                warn_rows.append(
                    (
                        run_id,
                        "indicator_coverage",
                        ticker,
                        "warn",
                        json.dumps({"last_bar_date": last_bar.isoformat()}),
                        checked_at,
                    )
                )

        _batch_insert_results(conn, warn_rows)
        return {
            "ok": ok_count,
            "warn": warned,
            "total": len(rows),
        }
    finally:
        conn.close()


@task(trigger_rule="all_done")
def summary() -> dict[str, str]:
    """
    Aggregate check results into one ``daily_summary`` row.

    The overall status is ``fail`` if any check failed, ``warn`` if any
    check warned, and ``ok`` otherwise.
    """
    ctx = get_current_context()
    run_id = _run_id(ctx)

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT check_name, status FROM massive.spot_check_results WHERE run_id = %s",
                (run_id,),
            )
            rows = cur.fetchall()

        has_fail = any(r[1] == "fail" for r in rows)
        has_warn = any(r[1] == "warn" for r in rows)

        status = "fail" if has_fail else "warn" if has_warn else "ok"

        by_check: dict[str, dict[str, int]] = {}
        for check_name, s in rows:
            bucket = by_check.setdefault(check_name, {"ok": 0, "warn": 0, "fail": 0})
            bucket[s] += 1

        _batch_insert_results(
            conn,
            [
                (
                    run_id,
                    "daily_summary",
                    None,
                    status,
                    json.dumps(
                        {
                            "by_check": by_check,
                            "overall": status,
                        }
                    ),
                    datetime.datetime.now(tz=UTC),
                )
            ],
        )

        return {"status": status}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def _chain_tasks(*tasks: object) -> None:
    """Set downstream dependencies between Airflow tasks in declaration order."""
    for i in range(len(tasks) - 1):
        tasks[i].set_downstream(tasks[i + 1])  # type: ignore[attr-defined]


def build_spot_check_group(dag: DAG) -> TaskGroup:
    """
    Build and return a ``spot_check`` TaskGroup with six DQ check tasks.

    The returned group must be wired into the DAG by the caller (e.g., via
    ``upstream_task >> group``).  No tasks are registered at module level.
    """
    with TaskGroup(group_id="spot_check", dag=dag) as group:
        _chain_tasks(
            freshness(),
            continuity(),
            survivorship_audit(),
            split_factor_assertion(),
            indicator_coverage(),
            summary(),
        )
    return group
