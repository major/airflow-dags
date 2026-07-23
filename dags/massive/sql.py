"""
DDL constants, split-adjustment SQL, and a bulk-upsert SQL builder for the ``massive`` schema.

All DDL is idempotent (``CREATE TABLE IF NOT EXISTS``, ``CREATE INDEX IF NOT EXISTS``)
so that first-run safety comes from re-execution rather than a separate bootstrap step.
Every public name in this module is a constant or pure function — no side effects.
"""

from __future__ import annotations

from psycopg2.sql import SQL, Composable, Identifier

SCHEMA_NAME = "massive"

# ---------------------------------------------------------------------------
# DDL — 6 tables in dependency order
# ---------------------------------------------------------------------------

CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS massive;

CREATE TABLE IF NOT EXISTS massive.tickers (
    ticker           text PRIMARY KEY,
    name             text,
    primary_exchange text,
    active           boolean,
    cik              text,
    composite_figi   text,
    currency_name    text,
    last_updated_utc timestamptz,
    refreshed_at     timestamptz,
    delisted_date    date,
    last_bar_date    date,
    first_bar_date   date,
    delisted_reason  text
);

CREATE INDEX IF NOT EXISTS massive_tickers_active_delisted
    ON massive.tickers (active, delisted_date);

CREATE TABLE IF NOT EXISTS massive.prices_raw (
    ticker       text NOT NULL,
    bar_date     date NOT NULL,
    open         numeric(20,8) NOT NULL,
    high         numeric(20,8) NOT NULL,
    low          numeric(20,8) NOT NULL,
    close        numeric(20,8) NOT NULL,
    volume       bigint NOT NULL,
    vwap         numeric(20,8),
    transactions bigint,
    ingested_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, bar_date)
);

CREATE TABLE IF NOT EXISTS massive.splits (
    event_id                     text PRIMARY KEY,
    ticker                       text NOT NULL,
    execution_date               date NOT NULL,
    split_from                   numeric(20,8) NOT NULL,
    split_to                     numeric(20,8) NOT NULL,
    split_ratio                  numeric(20,8)
        GENERATED ALWAYS AS (
            CASE
                WHEN split_from = 0 THEN NULL
                ELSE split_to / split_from
            END
        ) STORED,
    CONSTRAINT massive_splits_positive CHECK (split_from > 0 AND split_to > 0),
    adjustment_type              text,
    historical_adjustment_factor numeric(20,8),
    ingested_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS massive_splits_ticker_execution
    ON massive.splits (ticker, execution_date);

CREATE TABLE IF NOT EXISTS massive.prices_adjusted (
    ticker         text NOT NULL,
    bar_date       date NOT NULL,
    open           numeric(20,8) NOT NULL,
    high           numeric(20,8) NOT NULL,
    low            numeric(20,8) NOT NULL,
    close          numeric(20,8) NOT NULL,
    volume         bigint NOT NULL,
    vwap           numeric(20,8),
    transactions   bigint,
    ingested_at    timestamptz NOT NULL DEFAULT now(),
    adjusted_open  numeric(20,8) NOT NULL,
    adjusted_high  numeric(20,8) NOT NULL,
    adjusted_low   numeric(20,8) NOT NULL,
    adjusted_close numeric(20,8) NOT NULL,
    PRIMARY KEY (ticker, bar_date)
);

CREATE TABLE IF NOT EXISTS massive.indicators (
    ticker      text NOT NULL,
    bar_date    date NOT NULL,
    sma_20      numeric(20,8),
    sma_50      numeric(20,8),
    sma_200     numeric(20,8),
    ema_20      numeric(20,8),
    ema_50      numeric(20,8),
    ema_200     numeric(20,8),
    rsi_14      numeric(20,8),
    macd_line   numeric(20,8),
    macd_signal numeric(20,8),
    macd_hist   numeric(20,8),
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, bar_date)
);

CREATE TABLE IF NOT EXISTS massive.spot_check_results (
    check_id   bigserial PRIMARY KEY,
    run_id     text NOT NULL,
    check_name text NOT NULL,
    ticker     text,
    status     text NOT NULL,
    detail     jsonb,
    checked_at timestamptz NOT NULL DEFAULT now()
);
"""

# ---------------------------------------------------------------------------
# Apply-splits — full rebuild of prices_adjusted
# ---------------------------------------------------------------------------
# The adjustment factor is the cumulative product of split_to / split_from
# for every split with execution_date AFTER the bar date.  Pre-split prices
# are multiplied up so they are comparable with post-split prices.
#
# Example:
#   A bar at 2023-05-30, close $10.
#   A 2:1 split executes on 2023-06-01 (split_from=1, split_to=2).
#   Cumulative factor = 2/1 = 2.0 → adjusted close = $20.
#   If there were two splits (2:1 then 3:1), cumulative factor = 6.0.
#
# EXP(SUM(LN(x))) computes the product across rows in SQL.
# NULLIF guards against division by zero in split_from (a split with
# split_from=0 is degenerate — the ratio column returns NULL and the
# factor becomes NULL, which COALESCE converts to 1 = no adjustment).
#
# This applies to all instrument types (common stocks AND ETFs) because
# the split table is universal — the logic is the same.

APPLY_SPLITS_SQL = """
TRUNCATE massive.prices_adjusted;

INSERT INTO massive.prices_adjusted (
    ticker, bar_date,
    open, high, low, close, volume, vwap, transactions, ingested_at,
    adjusted_open, adjusted_high, adjusted_low, adjusted_close
)
SELECT
    r.ticker,
    r.bar_date,
    r.open,
    r.high,
    r.low,
    r.close,
    r.volume,
    r.vwap,
    r.transactions,
    r.ingested_at,
    r.open  * COALESCE(factor.cumulative_adj, 1::numeric),
    r.high  * COALESCE(factor.cumulative_adj, 1::numeric),
    r.low   * COALESCE(factor.cumulative_adj, 1::numeric),
    r.close * COALESCE(factor.cumulative_adj, 1::numeric)
FROM massive.prices_raw r
LEFT JOIN LATERAL (
    SELECT
        EXP(SUM(LN(s.split_to / NULLIF(s.split_from, 0::numeric))))
            AS cumulative_adj
    FROM massive.splits s
    WHERE s.ticker = r.ticker
      AND s.execution_date > r.bar_date
) factor ON true;
"""


def bulk_upsert_sql(
    table: str,
    columns: list[str],
    conflict_keys: list[str],
) -> Composable:
    """
    Build a parameterized ``INSERT ... ON CONFLICT DO UPDATE`` SQL template.

    The returned :class:`psycopg2.sql.Composable` contains a ``VALUES %s``
    placeholder suitable for use with :func:`psycopg2.extras.execute_values`.

    Parameters
    ----------
    table:
        Unqualified table name (e.g. ``"tickers"``).  The ``massive`` schema
        is prepended automatically.
    columns:
        Column names to insert.
    conflict_keys:
        Column names that form the conflict target (typically the primary key).

    Returns
    -------
    Composable
        SQL of the form::

            INSERT INTO massive.<table> (<col>, ...) VALUES %s
            ON CONFLICT (<key>, ...) DO UPDATE SET <col> = EXCLUDED.<col>, ...

    Raises
    ------
    ValueError
        If *table*, *columns*, or *conflict_keys* is empty.

    """
    if not table:
        msg = "table must be non-empty"
        raise ValueError(msg)
    if not columns:
        msg = "columns must be non-empty"
        raise ValueError(msg)
    if not conflict_keys:
        msg = "conflict_keys must be non-empty"
        raise ValueError(msg)

    col_idents = [Identifier(c) for c in columns]
    key_idents = [Identifier(k) for k in conflict_keys]

    # Every column that is NOT a conflict key gets an UPDATE assignment.
    update_cols = [c for c in columns if c not in conflict_keys]
    if update_cols:
        update_set = SQL(", ").join(
            SQL("{} = EXCLUDED.{}").format(Identifier(c), Identifier(c)) for c in update_cols
        )
        suffix = SQL("ON CONFLICT ({keys}) DO UPDATE SET {updates}").format(
            keys=SQL(", ").join(key_idents),
            updates=update_set,
        )
    else:
        # No non-key columns — conflict is a no-op.
        # This guards against invalid ``DO UPDATE SET`` with nothing to set
        # (all columns are conflict keys).  All current call sites have at
        # least one non-key column, so this branch is defensive.
        suffix = SQL("ON CONFLICT ({keys}) DO NOTHING").format(
            keys=SQL(", ").join(key_idents),
        )

    return SQL("INSERT INTO {table} ({cols}) VALUES %s {suffix}").format(
        table=Identifier(SCHEMA_NAME, table),
        cols=SQL(", ").join(col_idents),
        suffix=suffix,
    )
