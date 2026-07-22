"""Minimal example DAG to verify the Git DAG bundle is wired up correctly."""

from __future__ import annotations

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="example_dag_bundle_healthcheck",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    EmptyOperator(task_id="noop")
