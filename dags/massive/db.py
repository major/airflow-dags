"""
Postgres connection and bulk-operations helpers for the massive ETL.

All functions are safe to call from Airflow task bodies — there is no
module-level connection caching (Airflow task instances must not share
connections across runs).
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from airflow.sdk import BaseHook
from massive.sql import bulk_upsert_sql

logger = logging.getLogger(__name__)

_DEFAULT_PG_CONN_ID = "postgres_massive"


def get_pg_conn() -> psycopg2.extensions.connection:
    """
    Return a fresh Postgres connection from the ``postgres_massive`` Airflow connection.

    The connection string is built from the Airflow Connection's host, port,
    schema (database name), login, and password fields.

    Returns
    -------
    psycopg2.extensions.connection
        A new connection with ``connect_timeout=10`` and
        ``application_name=massive_etl``.

    """
    conn = BaseHook.get_connection(_DEFAULT_PG_CONN_ID)
    dsn = (
        f"host={conn.host} port={conn.port or 5432} dbname={conn.schema} "
        f"user={conn.login} password={conn.password} "
        f"connect_timeout=10 application_name=massive_etl"
    )
    return psycopg2.connect(dsn)


def bulk_upsert(
    connection: psycopg2.extensions.connection,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    conflict_keys: list[str],
) -> int:
    """
    Bulk-upsert *rows* into *table* using ``INSERT … ON CONFLICT DO UPDATE``.

    Uses :func:`psycopg2.extras.execute_values` with ``page_size=1000``.
    The operation runs inside a single transaction committed before returning.

    Parameters
    ----------
    connection:
        An open Postgres connection.
    table:
        Unqualified table name (schema ``massive`` is prepended automatically).
    columns:
        Column names in the same order as each row tuple.
    rows:
        Sequence of value tuples to insert.  May be empty (returns 0).
    conflict_keys:
        Column names that form the conflict target (typically the primary key).

    Returns
    -------
    int
        Number of rows written (always ``len(rows)``).

    """
    if not rows:
        return 0

    sql_template = bulk_upsert_sql(table, columns, conflict_keys)

    with connection.cursor() as cur:
        sql_str = sql_template.as_string(cur)
        execute_values(cur, sql_str, rows, page_size=1000)

    connection.commit()
    return len(rows)


def execute_script(connection: psycopg2.extensions.connection, sql: str) -> None:
    """
    Execute a multi-statement SQL string in a single transaction.

    The *sql* string is passed directly to ``cursor.execute()``.  This is safe
    for the DDL and TRUNCATE+INSERT scripts in this project — they contain no
    ``$$`` dollar-quoting or ``%s`` parameter substitution placeholders.

    Parameters
    ----------
    connection:
        An open Postgres connection.
    sql:
        One or more SQL statements (e.g. ``CREATE_TABLES_SQL``).

    """
    with connection.cursor() as cur:
        cur.execute(sql)
    connection.commit()
