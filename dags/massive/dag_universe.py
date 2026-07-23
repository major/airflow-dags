"""
Weekly refresh of the US common-stock + ETF universe from Massive.

Performs a soft-delete of tickers that fell out of the active listing,
stamps ``delisted_date`` using the 30-day heuristic, and updates
``last_bar_date`` / ``first_bar_date`` for every ticker from
``prices_raw``.

Scheduled Sun 02:00 America/New_York (off-peak for the Massive API).
Manual trigger also supported for emergency refreshes.

**Ordering note:** ``last_bar_date`` and ``first_bar_date`` are populated
from ``prices_raw``.  This DAG must run *after* the backfill has covered
the relevant date range for these values to be meaningful.  Until then
the columns will be NULL and the 30-day soft-delete will not fire.
"""

from __future__ import annotations

import datetime

from airflow.sdk import DAG, task
from massive.client import MassiveClient
from massive.db import bulk_upsert, get_pg_conn

UTC = datetime.UTC

DAG_ID = "massive_refresh_universe"

# Columns for the active-ticker upsert (``bulk_upsert``).  Lifecycle
# columns (``delisted_date``, ``delisted_reason``) are deliberately
# excluded so that a re-upserted active ticker never has its delisted
# state inadvertently cleared — the soft-delete step alone manages
# lifecycle transitions.  ``first_bar_date`` and ``last_bar_date`` are
# computed from ``prices_raw`` in a separate pass.
_TICKER_UPSERT_COLUMNS = [
    "ticker",
    "name",
    "type",
    "market",
    "locale",
    "primary_exchange",
    "active",
    "cik",
    "composite_figi",
    "currency_name",
    "last_updated_utc",
    "refreshed_at",
]


@task
def refresh_universe() -> dict[str, int]:
    """Pull the active universe, upsert it, refresh bar dates, apply soft-delete."""
    # ------------------------------------------------------------------
    # Step a — Pull the active universe from Massive and upsert.
    # ------------------------------------------------------------------
    client = MassiveClient.from_env()
    tickers = client.list_tickers(
        market="stocks",
        locale="us",
        types=("CS", "ETF"),
        active=True,
        page_size=1000,
    )

    # Safety guard: an empty response would soft-delete every known ticker.
    if not tickers:
        msg = "list_tickers returned zero tickers; refusing to proceed to avoid bulk delisting"
        raise RuntimeError(msg)

    pulled_tickers = {t.ticker for t in tickers}
    now = datetime.datetime.now(tz=UTC)

    # Build row tuples matching _TICKER_UPSERT_COLUMNS order.
    rows = [
        (
            t.ticker,
            t.name,
            t.type,
            t.market,
            t.locale,
            t.primary_exchange,
            True,  # active — we only pull active=true
            t.cik,
            t.composite_figi,
            t.currency_name,
            t.last_updated_utc,
            now,
        )
        for t in tickers
    ]

    conn = get_pg_conn()
    try:
        # Step a — upsert the active universe.
        # Committed immediately so pulled data survives a later-step
        # failure.  The soft-delete will be re-applied next week.
        bulk_upsert(
            conn,
            "tickers",
            _TICKER_UPSERT_COLUMNS,
            rows,
            conflict_keys=["ticker"],
        )
        conn.commit()

        # ------------------------------------------------------------------
        # Step b — Refresh first/last bar dates from prices_raw.
        # ------------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE massive.tickers t SET
                    last_bar_date  = sub.last_bar,
                    first_bar_date = sub.first_bar
                FROM (
                    SELECT ticker,
                           MAX(bar_date) AS last_bar,
                           MIN(bar_date) AS first_bar
                    FROM massive.prices_raw
                    GROUP BY ticker
                ) sub
                WHERE t.ticker = sub.ticker
            """)
        conn.commit()

        # ------------------------------------------------------------------
        # Step c — Resurrect cleanup + soft-delete.
        # ------------------------------------------------------------------
        # Resurrected tickers: a ticker was previously soft-deleted but is
        # now back in the active universe; clear its delisted columns.
        # Soft-delete: tickers NOT in the pulled response whose last bar is
        # older than 30 calendar days get stamped as delisted.
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE massive.tickers
                SET delisted_date  = NULL,
                    delisted_reason = NULL
                WHERE active = true
                  AND delisted_date IS NOT NULL
            """)

            pulled_list = list(pulled_tickers)
            cur.execute(
                """
                UPDATE massive.tickers
                SET active          = false,
                    delisted_date   = last_bar_date,
                    delisted_reason = 'fell_out_of_universe'
                WHERE active = true
                  AND delisted_date IS NULL
                  AND last_bar_date IS NOT NULL
                  AND last_bar_date < CURRENT_DATE - INTERVAL '30 days'
                  AND NOT (ticker = ANY(%s))
                """,
                (pulled_list,),
            )
            deleted_count = cur.rowcount
        conn.commit()

        # Total count for the return dict.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM massive.tickers")
            count_row = cur.fetchone()
            total_count = count_row[0] if count_row is not None else 0

    finally:
        conn.close()

    return {
        "active_pulled": len(tickers),
        "soft_deleted_this_run": deleted_count,
        "total_in_tickers_table": total_count,
    }


with DAG(
    dag_id=DAG_ID,
    schedule="0 2 * * 0",
    start_date=datetime.datetime(2024, 1, 1),
    timezone="America/New_York",
    catchup=False,
    max_active_runs=1,
    tags=["massive", "etl", "universe"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
        "execution_timeout": datetime.timedelta(minutes=30),
    },
) as dag:
    refresh_universe()
