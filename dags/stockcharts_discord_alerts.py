"""
Poll StockCharts alerts and send new ones to Discord.

This DAG fetches alerts from StockCharts every 5 minutes (Monday-Friday),
filters for new alerts since the last successful run, and sends them to Discord
via incoming webhooks.

The webhook URL(s) are never hardcoded here. They're read from an Airflow
connection (default: `stockcharts_discord_webhook`; see README) whose password
holds the webhook URL(s), so the secret lives outside this repo.

To target a different Discord webhook, add another `AIRFLOW_CONN_<CONN_ID>` key
to the `airflow-connections` secret and pass that connection id via this DAG's
`conn_id` param when triggering — no code change needed here.
"""

from __future__ import annotations

import datetime
import logging
import zoneinfo
from typing import Any

import requests

from airflow.sdk import DAG, BaseHook, Param, get_current_context, task

try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable

logger = logging.getLogger(__name__)

DEFAULT_CONN_ID = "stockcharts_discord_webhook"
STOCKCHARTS_URL = "https://stockcharts.com/j-sum/sum?cmd=alert"
STOCKCHARTS_TZ = zoneinfo.ZoneInfo("America/New_York")
NO_ALERTS_PLACEHOLDER = "There are no alerts today"
VARIABLE_NAME = "stockcharts_last_successful_run"


def parse_stockcharts_time(value: str) -> datetime.datetime:
    """
    Parse a StockCharts timestamp in America/New_York timezone.

    Strips trailing " ET" (case-sensitive), then parses against known formats:
    - "31 Jul 2024, 2:31pm" (lowercase am/pm, no space)
    - "1 Aug 2024, 8:11 AM" (uppercase AM/PM, with space)

    Raises ValueError if the timestamp cannot be parsed.
    """
    cleaned = value.strip()
    cleaned = cleaned.removesuffix(" ET").strip()

    formats = [
        "%d %b %Y, %I:%M%p",  # lowercase am/pm, no space
        "%d %b %Y, %I:%M %p",  # uppercase AM/PM, with space
    ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(cleaned, fmt).replace(tzinfo=STOCKCHARTS_TZ)
        except ValueError:
            continue

    msg = f"unsupported StockCharts timestamp: {value}"
    raise ValueError(msg)


@task(retries=2, retry_delay=datetime.timedelta(seconds=30))
def detect_changes() -> list[dict[str, Any]]:  # noqa: C901, PLR0912, PLR0915 - complex by design; refactor candidate
    """
    Fetch StockCharts alerts and filter for new ones since the last run.

    Returns a list of new alert dicts with keys: alert, bearish, lastfired, symbol.
    """
    # Capture the current time at the start of this task run (for anchor update).
    now = datetime.datetime.now(STOCKCHARTS_TZ)

    # Read the state anchor from Airflow Variable.
    try:
        anchor_str = Variable.get(VARIABLE_NAME)
        anchor = datetime.datetime.fromisoformat(anchor_str)
        # Ensure anchor is timezone-aware (in case it was stored without tz info).
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=STOCKCHARTS_TZ)
    except Exception:
        # If the variable doesn't exist or is malformed, default to now - 5 minutes.
        anchor = now - datetime.timedelta(minutes=5)
        logger.info(
            "Variable %s not found or invalid; defaulting anchor to now - 5 minutes",
            VARIABLE_NAME,
        )

    # Fetch alerts from StockCharts.
    try:
        response = requests.get(
            STOCKCHARTS_URL,
            headers={
                "Referer": "https://stockcharts.com/freecharts/alertsummary.html",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:129.0) Gecko/20100101 Firefox/129.0"
                ),
            },
            timeout=10,
        )
        response.raise_for_status()
        raw_alerts = response.json()
    except Exception:
        logger.exception("Failed to fetch StockCharts alerts")
        raise

    # Parse and filter alerts.
    parsed_alerts = []
    for raw_alert in raw_alerts:
        try:
            # Apply defaults and trim fields.
            alert_text = raw_alert.get("alert", "")
            alert_text = alert_text.strip() if isinstance(alert_text, str) else ""

            bearish = raw_alert.get("bearish", "no")
            bearish = bearish.strip() if isinstance(bearish, str) else "no"
            if not bearish:
                bearish = "no"

            lastfired = raw_alert.get("lastfired", "")
            lastfired = lastfired.strip() if isinstance(lastfired, str) else ""

            symbol = raw_alert.get("symbol", "UNKNOWN")
            symbol = symbol.strip() if isinstance(symbol, str) else "UNKNOWN"
            if not symbol:
                symbol = "UNKNOWN"

            # Skip the "no alerts today" placeholder.
            if alert_text == NO_ALERTS_PLACEHOLDER:
                continue

            # Parse the timestamp.
            try:
                fired_at = parse_stockcharts_time(lastfired)
            except ValueError as e:
                logger.warning("Failed to parse timestamp for symbol %s: %s", symbol, e)
                continue

            # Only keep alerts newer than the anchor.
            if fired_at > anchor:
                parsed_alerts.append(
                    {
                        "alert": alert_text,
                        "bearish": bearish,
                        "lastfired": lastfired,
                        "symbol": symbol,
                        "_fired_at": fired_at,  # Internal field for dedup
                    },
                )
        except Exception as e:
            logger.warning("Skipping malformed StockCharts alert: %s", e)
            continue

    # Dedup: keep only the latest-fired alert(s) per symbol.
    deduped = []
    for candidate in parsed_alerts:
        is_latest = True
        for other in parsed_alerts:
            if (
                other["symbol"] == candidate["symbol"]
                and other["_fired_at"] > candidate["_fired_at"]
            ):
                is_latest = False
                break
        if is_latest:
            deduped.append(candidate)

    # Remove the internal _fired_at field before returning.
    result = [{k: v for k, v in alert.items() if k != "_fired_at"} for alert in deduped]

    # Update the anchor to the current time (even if no new alerts were found).
    Variable.set(VARIABLE_NAME, now.isoformat())
    logger.info("Updated %s to %s", VARIABLE_NAME, now.isoformat())

    return result


@task
def send_to_discord(alerts: list[dict[str, Any]]) -> None:
    """
    Send alerts to Discord webhooks.

    Reads the webhook URL(s) from the connection's password field.
    Logs and continues on per-webhook failures; never raises.
    """
    conn_id = get_current_context()["params"]["conn_id"]
    webhook_urls_str = BaseHook.get_connection(conn_id).password

    if not webhook_urls_str:
        msg = (
            f"Connection '{conn_id}' has no password set; "
            "store the Discord webhook URL(s) there (comma-separated)."
        )
        raise ValueError(
            msg,
        )

    # Parse webhook URLs: split on comma, strip whitespace, drop empties, dedupe.
    webhook_urls = [url.strip() for url in webhook_urls_str.split(",") if url.strip()]
    webhook_urls = list(dict.fromkeys(webhook_urls))  # Dedupe while preserving order.

    if not alerts:
        logger.info("No alerts to send")
        return

    for alert in alerts:
        # Build the Discord payload.
        emoji = "🔴" if alert["bearish"] == "yes" else "💚"
        alert_text = alert["alert"]

        # Rewrite "Dow crosses above " prefix.
        dow_prefix = "Dow crosses above "
        if alert_text.startswith(dow_prefix):
            remainder = alert_text[len(dow_prefix) :]
            alert_text = f"THE DOW, THE DOW IS ABOVE {remainder}"

        content = f"{emoji}  {alert_text}"

        payload = {
            "username": alert["symbol"],
            "avatar_url": "https://emojiguide.org/images/emoji/1/8z8e40kucdd1.png",
            "content": content,
        }

        # Send to each webhook URL.
        for i, webhook_url in enumerate(webhook_urls, 1):
            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    timeout=10,
                )
                response.raise_for_status()
                logger.info(
                    "Alert sent to Discord",
                    extra={
                        "webhook": i,
                        "total": len(webhook_urls),
                        "symbol": alert["symbol"],
                    },
                )
            except Exception as e:
                logger.exception(
                    "Discord webhook failed",
                    extra={
                        "webhook": i,
                        "total": len(webhook_urls),
                        "symbol": alert["symbol"],
                        "error": str(e),
                    },
                )


with DAG(
    dag_id="stockcharts_discord_alerts",
    schedule="*/5 * * * 1-5",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["discord", "stockcharts"],
    params={
        "conn_id": Param(
            DEFAULT_CONN_ID,
            type="string",
            title="Airflow connection id",
            description="Connection holding the target Discord webhook URL(s), comma-separated.",
        ),
    },
) as dag:
    alerts = detect_changes()
    send_to_discord(alerts)
