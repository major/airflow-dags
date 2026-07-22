"""
Send a one-off test message to a Discord channel via an incoming webhook.

The webhook URL is never hardcoded here. It's read from an Airflow
connection (default: `discord_webhook`; see README) whose password holds
the webhook URL, so the secret lives outside this repo.

To target a different Discord webhook (e.g. a second channel), add another
`AIRFLOW_CONN_<CONN_ID>` key to the `airflow-connections` secret and pass
that connection id via this DAG's `conn_id` param when triggering — no code
change needed here.
"""

from __future__ import annotations

import datetime

import requests

from airflow.sdk import DAG, BaseHook, Param, get_current_context, task

DEFAULT_CONN_ID = "discord_webhook"


@task
def send_test_message() -> None:
    """Post a one-off test message to the configured Discord webhook."""
    conn_id = get_current_context()["params"]["conn_id"]
    webhook_url = BaseHook.get_connection(conn_id).password
    if not webhook_url:
        msg = f"Connection '{conn_id}' has no password set; store the Discord webhook URL there."
        raise ValueError(
            msg,
        )

    response = requests.post(
        webhook_url,
        json={"content": "Test message from Airflow 🚀"},
        timeout=10,
    )
    response.raise_for_status()


with DAG(
    dag_id="discord_test_message",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["discord", "example"],
    params={
        "conn_id": Param(
            DEFAULT_CONN_ID,
            type="string",
            title="Airflow connection id",
            description="Connection holding the target Discord webhook URL.",
        ),
    },
) as dag:
    send_test_message()
