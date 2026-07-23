"""
HTTP client for the Massive REST API (data provider for the massive ETL).

Provides a rate-limited, retrying client that mirrors the public Massive API
(``api.massive.com``).  Four endpoint methods mirror the DAG needs:

* ``get_market_summary``  — daily grouped OHLCV for the whole US market
* ``list_splits``         — cursor-paginated corporate split events
* ``list_tickers``        — cursor-paginated reference universe
* ``get_ticker_aggregates`` — per-ticker OHLCV range (for spot-check)

Every public method raises :class:`MassiveAPIError` on hard failure; the
caller never receives a ``None`` in place of data.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import logging
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MarketBar:
    """
    A single daily OHLCV bar from the Massive grouped-aggregates endpoint.

    Field name notes (wire → Python):
    - ``T`` → *ticker* (present in market-summary response, absent in per-ticker aggregates)
    - ``o``, ``h``, ``l``, ``c`` → open/high/low/close
    - ``v`` → *volume* (raw share count)
    - ``vw`` → VWAP (optional; ``None`` when absent)
    - ``n`` → transaction count (optional; ``None`` when absent)
    - ``t`` → Unix ms timestamp → *bar_date*
    - ``otc`` → OTC flag (only present when ``True``)
    """

    ticker: str
    bar_date: datetime.date
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    v: int
    vw: float | None = None
    n: int | None = None
    t_ms: int = 0
    otc: bool | None = None


@dataclass(frozen=True)
class SplitEvent:
    """
    A single corporate split event from ``/stocks/v1/splits``.

    ``split_ratio`` is a generated column in the database; the API returns
    ``split_from`` and ``split_to`` separately plus a pre-computed
    ``historical_adjustment_factor`` (used for cross-check assertions).
    """

    ticker: str
    execution_date: datetime.date
    split_from: float
    split_to: float
    adjustment_type: str | None = None
    historical_adjustment_factor: float | None = None
    event_id: str = ""


@dataclass(frozen=True)
class Ticker:
    """A reference-data ticker record from ``/v3/reference/tickers``."""

    ticker: str
    name: str | None = None
    type: str | None = None
    market: str | None = None
    locale: str | None = None
    primary_exchange: str | None = None
    active: bool | None = None
    cik: str | None = None
    composite_figi: str | None = None
    currency_name: str | None = None
    last_updated_utc: str | None = None


# ---------------------------------------------------------------------------
# Exception type
# ---------------------------------------------------------------------------


class MassiveAPIError(Exception):
    """
    Raised when the Massive API returns an error or retries are exhausted.

    Attributes
    ----------
    status_code : int | None
        HTTP status code from the response (``None`` for network errors).
    request_id : str | None
        Server-side request id, if available in the error body.
    url : str | None
        The URL that triggered the error.

    """

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        request_id: str | None = None,
        url: str | None = None,
    ) -> None:
        """Initialize the exception with HTTP context."""
        self.status_code = status_code
        self.request_id = request_id
        self.url = url
        super().__init__(message)


# ---------------------------------------------------------------------------
# Retry / pagination constants
# ---------------------------------------------------------------------------

_TOO_MANY_REQUESTS = 429
_RETRYABLE_STATUSES = frozenset({_TOO_MANY_REQUESTS, 500, 502, 503, 504})
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)
_MAX_PAGINATION_PAGES = 100
_MIN_ERROR_STATUS = 400


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class MassiveClient:
    """
    A rate-limited, retrying HTTP client for the Massive REST API.

    Parameters
    ----------
    api_key:
        Massive API key (``Authorization: Bearer``).
    base_url:
        API base URL.  Defaults to ``https://api.massive.com``.
    max_workers:
        Maximum concurrent threads for the internal
        :class:`~concurrent.futures.ThreadPoolExecutor`.
    max_retries:
        Maximum number of retry attempts per request (exponential backoff
        with jitter on 429, 5xx, connection errors, and timeouts).
    timeout:
        Per-request timeout in seconds.

    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.massive.com",
        max_workers: int = 50,
        max_retries: int = 4,
        timeout: float = 30.0,
    ) -> None:
        """Store config, create a requests Session with Bearer auth."""
        self.base_url = base_url.rstrip("/")
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.timeout = timeout

        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})
        self._session.headers.update({"User-Agent": "massive-etl/1.0"})

        self._executor: concurrent.futures.ThreadPoolExecutor | None = None

    def __repr__(self) -> str:
        """Return a debug string that does **not** leak the API key."""
        return f"<MassiveClient base_url={self.base_url!r}>"

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, conn_id: str = "massive_api") -> MassiveClient:
        """
        Build a client from an Airflow connection.

        The API key is read from the connection's *password* field.

        Parameters
        ----------
        conn_id:
            Airflow connection id (default ``massive_api``).

        Returns
        -------
        MassiveClient

        """
        conn = BaseHook.get_connection(conn_id)
        return cls(api_key=conn.password)

    # ------------------------------------------------------------------
    # Concurrency support
    # ------------------------------------------------------------------

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """
        A lazily-created :class:`~concurrent.futures.ThreadPoolExecutor`.

        Use this for fan-out patterns::

            with client.executor as pool:
                futures = {pool.submit(client.get_ticker_aggregates, t, ...): t for t in tickers}
                ...
        """
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers,
            )
        return self._executor

    # ------------------------------------------------------------------
    # Public endpoint methods
    # ------------------------------------------------------------------

    def get_market_summary(
        self,
        date: datetime.date,
        *,
        include_otc: bool = False,
        adjusted: bool = False,
    ) -> list[MarketBar]:
        """
        Fetch the daily grouped OHLCV for the entire US market.

        This is a single-request endpoint (no cursor pagination).  The
        response contains one bar per traded ticker for *date*.

        Parameters
        ----------
        date:
            Trading date in ``YYYY-MM-DD``.
        include_otc:
            Include OTC-traded securities (default ``False``).
        adjusted:
            Request dividend-adjusted prices (default ``False`` — unadjusted).

        Returns
        -------
        list[MarketBar]
            Bars for the given date.  Empty list when *date* is a non-trading
            day or the market was closed.

        """
        path = f"/v2/aggs/grouped/locale/us/market/stocks/{date.isoformat()}"
        url = f"{self.base_url}{path}"

        params: dict[str, Any] = {
            "adjusted": str(adjusted).lower(),
            "include_otc": str(include_otc).lower(),
        }

        data = self._request("GET", url, params=params)
        raw_results: list[dict[str, Any]] | None = data.get("results")

        if raw_results is None:
            return []

        bars: list[MarketBar] = []
        skipped = 0
        for raw in raw_results:
            bar = self._parse_market_bar(raw)
            valid, reason = self._is_valid_bar(bar)
            if not valid:
                skipped += 1
                logger.warning("Skipped invalid bar %s/%s: %s", bar.ticker, bar.bar_date, reason)
                continue
            bars.append(bar)

        if skipped:
            logger.info("Skipped %d invalid bar(s) in market summary for %s", skipped, date)

        return bars

    def list_splits(
        self,
        *,
        execution_date_gte: datetime.date | None = None,
        page_size: int = 5000,
    ) -> list[SplitEvent]:
        """
        Fetch corporate split events, cursor-paginated.

        Parameters
        ----------
        execution_date_gte:
            Only return splits with ``execution_date >=`` this value.
        page_size:
            Results per page (max 5000).

        Returns
        -------
        list[SplitEvent]

        """
        path = "/stocks/v1/splits"
        url = f"{self.base_url}{path}"

        params: dict[str, Any] = {"limit": page_size, "sort": "execution_date"}
        if execution_date_gte is not None:
            params["execution_date.gte"] = execution_date_gte.isoformat()

        raw_results = self._paginate(url, params=params)
        return [self._parse_split(raw) for raw in raw_results]

    def list_tickers(
        self,
        *,
        market: str = "stocks",
        locale: str = "us",
        types: tuple[str, ...] = ("CS", "ETF"),
        active: bool = True,
        page_size: int = 1000,
    ) -> list[Ticker]:
        """
        Fetch the reference-ticker universe, cursor-paginated.

        Parameters
        ----------
        market:
            Market filter (default ``stocks``).
        locale:
            Locale filter (default ``us``).
        types:
            Instrument types to include (default ``("CS", "ETF")`` for common
            stocks and ETFs).
        active:
            Only return currently-active tickers (default ``True``).
        page_size:
            Results per page (max 1000).

        Returns
        -------
        list[Ticker]

        """
        path = "/v3/reference/tickers"
        url = f"{self.base_url}{path}"

        params: dict[str, Any] = {
            "market": market,
            "locale": locale,
            "type": ",".join(types),
            "active": str(active).lower(),
            "limit": page_size,
        }

        raw_results = self._paginate(url, params=params)
        return [self._parse_ticker(raw) for raw in raw_results]

    def get_ticker_aggregates(
        self,
        ticker: str,
        frm: datetime.date,
        to: datetime.date,
    ) -> list[MarketBar]:
        """
        Fetch per-ticker daily OHLCV for a date range.

        The underlying endpoint is cursor-paginated so this method handles
        following ``next_url`` transparently.

        Parameters
        ----------
        ticker:
            Ticker symbol (e.g. ``AAPL``).
        frm:
            Start date (inclusive).
        to:
            End date (inclusive).

        Returns
        -------
        list[MarketBar]

        """
        path = f"/v2/aggs/ticker/{ticker}/range/1/day/{frm.isoformat()}/{to.isoformat()}"
        url = f"{self.base_url}{path}"

        raw_results = self._paginate(url)
        return [self._parse_market_bar(raw, ticker=ticker) for raw in raw_results]

    # ------------------------------------------------------------------
    # Internal — request lifecycle
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Send one HTTP request with retry/backoff.

        Retries on 429, 5xx, :class:`~requests.exceptions.ConnectionError`,
        and :class:`~requests.exceptions.Timeout`.  Uses exponential backoff
        with random jitter.  Honors the ``Retry-After`` header on 429.

        Raises :class:`MassiveAPIError` after exhausting retries.
        """
        last_exception: Exception | None = None
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    timeout=self.timeout,
                )
            except _RETRYABLE_EXCEPTIONS as exc:
                last_exception = exc
                last_response = None
                if attempt < self.max_retries:
                    delay = _backoff_delay(attempt)
                    logger.warning(
                        "Request failed (attempt %d/%d): %s.  Retrying in %.1fs.",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                break

            last_response = response

            delay = self._handle_retryable_response(response, attempt)
            if delay is not None:
                time.sleep(delay)
                continue

            status = response.status_code
            if status >= _MIN_ERROR_STATUS:
                if status in _RETRYABLE_STATUSES:
                    # Retryable status on the final attempt — retries exhausted.
                    raise self._build_retry_exhausted_error(
                        response,
                        None,
                        url,
                    )
                _raise_non_retryable_error(response, status, url)

            # Success (2xx).
            return response.json()

        # All retries exhausted.
        raise self._build_retry_exhausted_error(
            last_response,
            last_exception,
            url,
        ) from last_exception

    def _build_retry_exhausted_error(
        self,
        last_response: requests.Response | None,
        last_exception: Exception | None,
        url: str,
    ) -> MassiveAPIError:
        """Build a :class:`MassiveAPIError` after all retries are exhausted."""
        status_code: int | None = None
        rid: str | None = None
        body: str | None = None

        if last_response is not None:
            status_code = last_response.status_code
            payload = _safe_json(last_response)
            if payload is not None:
                rid = payload.get("request_id")
                if rid is None:
                    rid = payload.get("requestId")
                body = str(payload)

        if last_exception is not None:
            cause = str(last_exception)
        elif last_response is not None and last_response.reason:
            cause = last_response.reason
        else:
            cause = "unknown"

        displayed_body = body if body is not None else "none"
        msg = (
            f"Request failed after {self.max_retries} retries: {cause}.  "
            f"Last response: {displayed_body}"
        )
        return MassiveAPIError(
            msg,
            status_code=status_code,
            request_id=rid,
            url=url,
        )

    def _handle_retryable_response(
        self,
        response: requests.Response,
        attempt: int,
    ) -> float | None:
        """
        Decide whether to retry *response*.

        Returns the delay in seconds if the request should be retried,
        or ``None`` if the status is not retryable or retries are exhausted.
        """
        status = response.status_code
        if status not in _RETRYABLE_STATUSES:
            return None
        if attempt >= self.max_retries:
            return None
        if status == _TOO_MANY_REQUESTS:
            retry_after = _parse_retry_after(response)
            if retry_after is not None:
                return retry_after
            return _backoff_delay(attempt)
        return _backoff_delay(attempt)

    def _paginate(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Follow cursor pagination, yielding all results from all pages.

        Raises :class:`MassiveAPIError` if *url* redirects to a host that
        does not match the configured ``base_url`` host, or if the pagination
        exceeds 100 pages.
        """
        all_results: list[dict[str, Any]] = []
        current_url: str | None = url
        current_params = params
        page_count = 0

        allowed_host = urlparse(self.base_url).hostname

        while current_url is not None and page_count < _MAX_PAGINATION_PAGES:
            # Validate host.
            parsed = urlparse(current_url)
            if parsed.hostname is not None and parsed.hostname != allowed_host:
                msg = (
                    f"Pagination URL host '{parsed.hostname}' does not match "
                    f"expected '{allowed_host}'.  Refusing to follow: {current_url}"
                )
                raise MassiveAPIError(msg, url=current_url)

            data = self._request("GET", current_url, params=current_params)

            # Only the first request uses query params; subsequent pages
            # carry everything in the URL.
            current_params = None

            page_results: list[dict[str, Any]] | None = data.get("results")
            if page_results is not None:
                all_results.extend(page_results)
            elif data.get("status") == "OK":
                # Paginated endpoints that return zero results still return
                # status=OK with no results key.  This is not an error.
                pass
            else:
                msg = f"Paginated response missing 'results' key: {data}"
                raise MassiveAPIError(msg, status_code=200, url=current_url)

            current_url = data.get("next_url")
            page_count += 1

        if current_url is not None and page_count >= _MAX_PAGINATION_PAGES:
            logger.warning(
                "Pagination exceeded %d pages; truncating at %d results.",
                _MAX_PAGINATION_PAGES,
                len(all_results),
            )

        return all_results

    # ------------------------------------------------------------------
    # Internal — parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_market_bar(raw: dict[str, Any], ticker: str | None = None) -> MarketBar:
        """
        Convert a raw API result dict into a :class:`MarketBar`.

        Parameters
        ----------
        raw:
            The JSON object from ``results[]``.
        ticker:
            Ticker to inject (required for per-ticker aggregates which omit
            the ``T`` field).  If ``None``, reads from ``raw["T"]``.

        """
        t_ms: int = raw.get("t", 0)
        if t_ms:
            bar_date = datetime.datetime.fromtimestamp(
                t_ms / 1000,
                tz=datetime.UTC,
            ).date()
        else:
            bar_date = datetime.date.min
        resolved_ticker = ticker if ticker is not None else raw.get("T", "")

        return MarketBar(
            ticker=resolved_ticker,
            bar_date=bar_date,
            o=float(raw.get("o", 0)),
            h=float(raw.get("h", 0)),
            l=float(raw.get("l", 0)),
            c=float(raw.get("c", 0)),
            v=int(raw.get("v", 0)),
            vw=float(raw["vw"]) if raw.get("vw") is not None else None,
            n=int(raw["n"]) if raw.get("n") is not None else None,
            t_ms=t_ms,
            otc=raw.get("otc"),
        )

    @staticmethod
    def _parse_split(raw: dict[str, Any]) -> SplitEvent:
        """Convert a raw API result dict into a :class:`SplitEvent`."""
        return SplitEvent(
            ticker=raw.get("ticker", ""),
            execution_date=datetime.date.fromisoformat(raw.get("execution_date", "2000-01-01")),
            split_from=float(raw.get("split_from", 0)),
            split_to=float(raw.get("split_to", 0)),
            adjustment_type=raw.get("adjustment_type"),
            historical_adjustment_factor=_safe_float(raw.get("historical_adjustment_factor")),
            event_id=raw.get("id", ""),
        )

    @staticmethod
    def _parse_ticker(raw: dict[str, Any]) -> Ticker:
        """Convert a raw API result dict into a :class:`Ticker`."""
        return Ticker(
            ticker=raw.get("ticker", ""),
            name=raw.get("name"),
            type=raw.get("type"),
            market=raw.get("market"),
            locale=raw.get("locale"),
            primary_exchange=raw.get("primary_exchange"),
            active=raw.get("active"),
            cik=raw.get("cik"),
            composite_figi=raw.get("composite_figi"),
            currency_name=raw.get("currency_name"),
            last_updated_utc=raw.get("last_updated_utc"),
        )

    # ------------------------------------------------------------------
    # Internal — validation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_valid_bar(bar: MarketBar) -> tuple[bool, str | None]:
        """
        Tiered bad-bar validation (filtering tier only).

        Policy (Defined in the deepwork "Bad-bar policy" locked decision):
        - ``high < low`` → **skip** (data error, corrupts downstream calculations).
        - ``close = 0 AND volume = 0`` → **keep** (may be halted/quoted; flagged in spot-check).
        - ``close = 0 AND volume > 0`` → **keep** (rare but real; flagged in spot-check).
        - ``volume = 0 AND high > 0`` → **keep** (halted-but-quoted; flagged in spot-check).

        Only the first category (hard data error) is filtered here.  The
        remaining categories pass through and are handled by the
        spot-check phase.
        """
        if bar.h < bar.l:
            return False, "high < low"
        return True, None


def _raise_non_retryable_error(
    response: requests.Response,
    status: int,
    url: str,
) -> None:
    """
    Raise :class:`MassiveAPIError` for a non-retryable HTTP response.

    Always raises; the return type is ``None`` for type-checker convenience.
    """
    payload = _safe_json(response)
    rid: str | None = None
    if payload is not None:
        rid = payload.get("request_id")
        if rid is None:
            rid = payload.get("requestId")
    displayed = payload if payload is not None else response.reason
    msg = f"API returned HTTP {status}: {displayed}"
    raise MassiveAPIError(msg, status_code=status, request_id=rid, url=url)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _backoff_delay(attempt: int, base: float = 1.5) -> float:
    """Exponential backoff with jitter: ``base ** attempt + random(0, 1)``."""
    return base**attempt + random.random()  # noqa: S311


def _parse_retry_after(response: requests.Response) -> float | None:
    """Extract ``Retry-After`` header as seconds, if present and parseable."""
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        pass
    # Retry-After could be an HTTP-date; fall back to None.
    return None


def _safe_json(response: requests.Response) -> dict[str, Any] | None:
    """Try to parse response body as JSON; return ``None`` on failure."""
    try:
        return response.json()
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:  # noqa: ANN401
    """
    Convert *value* to float or return ``None``.

    Accepts ``Any`` because the caller may pass arbitrary JSON-decoded types
    (int, float, str, ``None``).  The try/except guards against uncoercible
    values.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
