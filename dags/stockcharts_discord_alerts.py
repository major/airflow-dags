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


def _load_anchor(now: datetime.datetime) -> datetime.datetime:
    """
    Read the state anchor from an Airflow Variable.

    Returns the stored timestamp if valid, otherwise ``now - 5 minutes``.
    """
    try:
        anchor_str = Variable.get(VARIABLE_NAME)
        anchor = datetime.datetime.fromisoformat(anchor_str)
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=STOCKCHARTS_TZ)
    except Exception:
        anchor = now - datetime.timedelta(minutes=5)
        logger.info(
            "Variable %s not found or invalid; defaulting anchor to now - 5 minutes",
            VARIABLE_NAME,
        )
    return anchor


def _fetch_alerts() -> list[dict[str, Any]]:
    """
    Fetch raw alerts from StockCharts.

    Returns the parsed JSON payload. Re-raises on any failure after logging.
    """
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
        return response.json()
    except Exception:
        logger.exception("Failed to fetch StockCharts alerts")
        raise


def _clean_alert_text(raw: object) -> str:
    """Clean the *alert* field: strip if str, else ``""``."""
    return raw.strip() if isinstance(raw, str) else ""


def _clean_alert_bearish(raw: object) -> str:
    """
    Clean the *bearish* field.

    Strip if str, else ``"no"``; empty after strip becomes ``"no"``.
    """
    if isinstance(raw, str):
        bearish = raw.strip()
        return bearish or "no"
    return "no"


def _clean_alert_symbol(raw: object) -> str:
    """
    Clean the *symbol* field.

    Strip if str, else ``"UNKNOWN"``; empty after strip becomes ``"UNKNOWN"``.
    """
    if isinstance(raw, str):
        symbol = raw.strip()
        return symbol or "UNKNOWN"
    return "UNKNOWN"


def _clean_alert_lastfired(raw: object) -> str:
    """Clean the *lastfired* field: strip if str, else ``""``."""
    return raw.strip() if isinstance(raw, str) else ""


def _parse_alert(
    raw: dict[str, Any],
    anchor: datetime.datetime,
) -> dict[str, Any] | None:
    """
    Parse a single raw alert dict.

    Cleans fields, skips the *no alerts* placeholder, parses the timestamp,
    and returns the alert dict (with ``_fired_at``) if newer than *anchor*.
    Returns ``None`` when the alert should be skipped.
    """
    try:
        alert_text = _clean_alert_text(raw.get("alert"))
        bearish = _clean_alert_bearish(raw.get("bearish"))
        lastfired = _clean_alert_lastfired(raw.get("lastfired"))
        symbol = _clean_alert_symbol(raw.get("symbol"))

        # Skip the "no alerts today" placeholder.
        if alert_text == NO_ALERTS_PLACEHOLDER:
            return None

        # Parse the timestamp.
        try:
            fired_at = parse_stockcharts_time(lastfired)
        except ValueError as e:
            logger.warning("Failed to parse timestamp for symbol %s: %s", symbol, e)
            return None

        # Only keep alerts newer than the anchor.
        if fired_at > anchor:
            return {
                "alert": alert_text,
                "bearish": bearish,
                "lastfired": lastfired,
                "symbol": symbol,
                "_fired_at": fired_at,
            }
    except Exception as e:
        logger.warning("Skipping malformed StockCharts alert: %s", e)
        return None
    return None


def _is_strictly_newer(
    candidate: dict[str, Any],
    others: list[dict[str, Any]],
) -> bool:
    """Return True when no alert in *others* has the same symbol and a newer timestamp."""
    for other in others:
        if other["symbol"] == candidate["symbol"] and other["_fired_at"] > candidate["_fired_at"]:
            return False
    return True


def _dedup_latest_per_symbol(
    alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    O(n²) dedup keeping only the latest-fired alert per symbol.

    When two alerts share a symbol the first one in the list is kept
    (tiebreaker: first match wins).
    """
    return [c for c in alerts if _is_strictly_newer(c, alerts)]


def _strip_internal_fields(
    alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Remove the internal ``_fired_at`` key from every alert dict."""
    return [{k: v for k, v in a.items() if k != "_fired_at"} for a in alerts]


@task(retries=2, retry_delay=datetime.timedelta(seconds=30))
def detect_changes() -> list[dict[str, Any]]:
    """
    Fetch StockCharts alerts and filter for new ones since the last run.

    Returns a list of new alert dicts with keys: alert, bearish, lastfired, symbol.
    """
    # Capture the current time at the start of this task run (for anchor update).
    now = datetime.datetime.now(STOCKCHARTS_TZ)

    anchor = _load_anchor(now)
    raw_alerts = _fetch_alerts()

    # Parse and filter alerts.
    parsed = []
    for raw_alert in raw_alerts:
        alert = _parse_alert(raw_alert, anchor)
        if alert is not None:
            parsed.append(alert)

    # Dedup: keep only the latest-fired alert(s) per symbol.
    deduped = _dedup_latest_per_symbol(parsed)
    result = _strip_internal_fields(deduped)

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
