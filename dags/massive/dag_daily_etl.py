"""
Daily ETL: ingest OHLCV + splits, rebuild adjusted prices, indicators, spot-check.

Scheduled Mon-Fri 17:00 America/New_York.  Empty ``ingest_ohlcv`` results
short-circuit (weekends/holidays return no market data).

Task chain::

    ingest_ohlcv ──┐
                    ├──► apply_splits ──► compute_indicators ──► spot_check
    ingest_splits ──┘
"""

from __future__ import annotations

import datetime
import logging
import math

import pandas as pd
import psycopg2

from airflow.sdk import DAG, Param, get_current_context, task
from massive import sql
from massive.client import MassiveClient
from massive.db import bulk_upsert, execute_script, get_pg_conn
from massive.spot_check import build_spot_check_group

logger = logging.getLogger(__name__)

UTC = datetime.UTC
DAG_ID = "massive_daily_etl"

_OHLCV_COLUMNS = [
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

_SPLIT_COLUMNS = [
    "event_id",
    "ticker",
    "execution_date",
    "split_from",
    "split_to",
    "adjustment_type",
    "historical_adjustment_factor",
    "ingested_at",
]

_INDICATOR_COLUMNS = [
    "ticker",
    "bar_date",
    "sma_20",
    "sma_50",
    "sma_200",
    "ema_20",
    "ema_50",
    "ema_200",
    "rsi_14",
    "macd_line",
    "macd_signal",
    "macd_hist",
    "computed_at",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_target_date(ctx: dict) -> datetime.date:
    """
    Extract the target trading date from the run context.

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


def _safe_float(value: object) -> float | None:
    """Convert a value to float or ``None``, handling NaN and None."""
    if value is None:
        return None
    try:
        v = float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError, OverflowError):
        return None
    if math.isnan(v):
        return None
    return v


def _wilder_rsi(series: pd.Series, window: int = 14) -> pd.Series:  # type: ignore[return-value]
    """Compute RSI using Wilder's smoothing (EMA with ``alpha = 1/window``)."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1.0 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / window, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi: pd.Series = 100.0 - (100.0 / (1.0 + rs))  # type: ignore[operator]
    rsi.iloc[:window] = None  # type: ignore[index]
    return rsi


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def ingest_ohlcv() -> dict:
    """
    Pull the market summary for the target trading day and upsert to ``prices_raw``.

    Also re-pulls the preceding 4 calendar days (late-correction window).
    Empty results (weekends/holidays) short-circuit without error.
    """
    ctx = get_current_context()
    target_day = _get_target_date(ctx)

    client = MassiveClient.from_env()
    bars = client.get_market_summary(target_day, adjusted=False, include_otc=False)

    if not bars:
        logger.info(
            "No market data for %s (holiday/weekend); short-circuiting.",
            target_day,
        )
        return {"rows_written": 0, "skipped": "empty"}

    ingested_at = datetime.datetime.now(tz=UTC)
    conn = get_pg_conn()
    try:
        rows = _build_ohlcv_rows(bars, target_day, ingested_at)
        count = bulk_upsert(
            conn,
            "prices_raw",
            _OHLCV_COLUMNS,
            rows,
            conflict_keys=["ticker", "bar_date"],
        )

        # Late-correction re-pull: re-fetch D-1 .. D-4 and upsert.
        for offset in range(1, 5):
            day = target_day - datetime.timedelta(days=offset)
            late_bars = client.get_market_summary(
                day,
                adjusted=False,
                include_otc=False,
            )
            if not late_bars:
                logger.info(
                    "Late-correction re-pull: no data for %s (skipping).",
                    day,
                )
                continue
            late_rows = _build_ohlcv_rows(late_bars, day, ingested_at)
            late_count = bulk_upsert(
                conn,
                "prices_raw",
                _OHLCV_COLUMNS,
                late_rows,
                conflict_keys=["ticker", "bar_date"],
            )
            count += late_count
            logger.info(
                "Late-correction re-pull for %s: %d bars upserted.",
                day,
                late_count,
            )

        return {"rows_written": count}
    finally:
        conn.close()


def _build_ohlcv_rows(
    bars: list,
    day: datetime.date,
    ingested_at: datetime.datetime,
) -> list[tuple]:
    """
    Convert ``MarketBar`` objects to ``(ticker, bar_date, ...)`` tuples.

    Applies a secondary ``_is_valid_bar`` filter (belt-and-suspenders; the
    client already filters during ``get_market_summary``).
    """
    rows: list[tuple] = []
    for bar in bars:
        ok, _ = MassiveClient._is_valid_bar(bar)  # noqa: SLF001
        if not ok:
            continue
        rows.append(
            (
                bar.ticker,
                day,
                bar.o,
                bar.h,
                bar.l,
                bar.c,
                bar.v,
                bar.vw,
                bar.n,
                ingested_at,
            )
        )
    return rows


@task
def ingest_splits() -> dict:
    """Pull the last 30 days of splits and upsert per-row (CheckViolation-safe)."""
    ctx = get_current_context()
    target_day = _get_target_date(ctx)

    client = MassiveClient.from_env()
    splits = client.list_splits(
        execution_date_gte=target_day - datetime.timedelta(days=30),
    )

    ingested_at = datetime.datetime.now(tz=UTC)
    conn = get_pg_conn()
    try:
        ingested = 0
        skipped = 0
        for split in splits:
            row = (
                split.event_id,
                split.ticker,
                split.execution_date,
                split.split_from,
                split.split_to,
                split.adjustment_type,
                split.historical_adjustment_factor,
                ingested_at,
            )
            try:
                bulk_upsert(
                    conn,
                    "splits",
                    _SPLIT_COLUMNS,
                    [row],
                    conflict_keys=["event_id"],
                )
                ingested += 1
            except psycopg2.errors.CheckViolation:
                logger.warning(
                    "CheckViolation for split %s/%s (split_from=%s, split_to=%s); skipping.",
                    split.event_id,
                    split.ticker,
                    split.split_from,
                    split.split_to,
                )
                conn.rollback()
                skipped += 1

        return {"splits_ingested": ingested, "splits_skipped": skipped}
    finally:
        conn.close()


@task(execution_timeout=datetime.timedelta(minutes=15))
def apply_splits(
    ingest_ohlcv_out: dict,  # noqa: ARG001
    ingest_splits_out: dict,  # noqa: ARG001
) -> dict:
    """Bootstrap tables and rebuild ``prices_adjusted`` from raw prices and splits."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '15min'")
        conn.commit()

        execute_script(conn, sql.CREATE_TABLES_SQL)
        execute_script(conn, sql.APPLY_SPLITS_SQL)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM massive.prices_adjusted")
            result = cur.fetchone()
            count = result[0] if result is not None else 0

        return {"rows_in_prices_adjusted": count}
    finally:
        conn.close()


@task(execution_timeout=datetime.timedelta(minutes=15))
def compute_indicators(prior: dict) -> dict:  # noqa: ARG001
    """Compute SMA/EMA/RSI/MACD from ``prices_adjusted`` and upsert to ``indicators``."""
    conn = get_pg_conn()
    try:
        df = pd.read_sql(
            "SELECT ticker, bar_date, adjusted_close "
            "FROM massive.prices_adjusted "
            "ORDER BY ticker, bar_date",
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return {"rows_in_indicators": 0}

    # SMA
    df["sma_20"] = (
        df.groupby("ticker")["adjusted_close"].rolling(20).mean().reset_index(0, drop=True)
    )
    df["sma_50"] = (
        df.groupby("ticker")["adjusted_close"].rolling(50).mean().reset_index(0, drop=True)
    )
    df["sma_200"] = (
        df.groupby("ticker")["adjusted_close"].rolling(200).mean().reset_index(0, drop=True)
    )

    # EMA
    df["ema_20"] = (
        df.groupby("ticker")["adjusted_close"]
        .ewm(span=20, adjust=False)
        .mean()
        .reset_index(0, drop=True)
    )
    df["ema_50"] = (
        df.groupby("ticker")["adjusted_close"]
        .ewm(span=50, adjust=False)
        .mean()
        .reset_index(0, drop=True)
    )
    df["ema_200"] = (
        df.groupby("ticker")["adjusted_close"]
        .ewm(span=200, adjust=False)
        .mean()
        .reset_index(0, drop=True)
    )

    # -- RSI (Wilder smoothing) --
    df["rsi_14"] = df.groupby("ticker")["adjusted_close"].transform(_wilder_rsi, window=14)

    # MACD
    ema_12 = (
        df.groupby("ticker")["adjusted_close"]
        .ewm(span=12, adjust=False)
        .mean()
        .reset_index(0, drop=True)
    )
    ema_26 = (
        df.groupby("ticker")["adjusted_close"]
        .ewm(span=26, adjust=False)
        .mean()
        .reset_index(0, drop=True)
    )
    df["macd_line"] = ema_12 - ema_26
    df["macd_signal"] = (
        df.groupby("ticker")["macd_line"].ewm(span=9, adjust=False).mean().reset_index(0, drop=True)
    )
    df["macd_hist"] = df["macd_line"] - df["macd_signal"]

    # Build rows for database upsert.
    computed_at = datetime.datetime.now(tz=UTC)
    rows: list[tuple] = []
    for _, row in df.iterrows():
        rows.append(
            (
                row["ticker"],
                row["bar_date"],
                _safe_float(row["sma_20"]),
                _safe_float(row["sma_50"]),
                _safe_float(row["sma_200"]),
                _safe_float(row["ema_20"]),
                _safe_float(row["ema_50"]),
                _safe_float(row["ema_200"]),
                _safe_float(row["rsi_14"]),
                _safe_float(row["macd_line"]),
                _safe_float(row["macd_signal"]),
                _safe_float(row["macd_hist"]),
                computed_at,
            )
        )

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE massive.indicators")
        conn.commit()

        bulk_upsert(
            conn,
            "indicators",
            _INDICATOR_COLUMNS,
            rows,
            conflict_keys=["ticker", "bar_date"],
        )
    finally:
        conn.close()

    return {"rows_in_indicators": len(rows)}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    schedule="0 17 * * 1-5",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    timezone="America/New_York",
    tags=["massive", "etl", "daily"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    params={
        "split_factor_mode": Param(
            default="auto",
            type="string",
            title="Split factor assertion mode",
            description=(
                "How to compare historical_adjustment_factor: "
                "'auto' (try both numerator and reciprocal, default), "
                "'numerator' (split_to/split_only), "
                "'reciprocal' (1 / (split_to/split_from))."
            ),
        ),
        "chunk_by_exchange": Param(
            default=False,
            type="boolean",
            title="Chunk by exchange (future optimization)",
            description=(
                "If true, indicators are computed per-exchange group "
                "rather than in a single DataFrame. Not yet implemented; "
                "reserved for future use."
            ),
        ),
    },
) as dag:
    ohlcv_out = ingest_ohlcv()
    splits_out = ingest_splits()
    apply_out = apply_splits(ohlcv_out, splits_out)
    indicators_out = compute_indicators(apply_out)
    spot_group = build_spot_check_group(dag)
    indicators_out >> spot_group  # type: ignore[operator]
