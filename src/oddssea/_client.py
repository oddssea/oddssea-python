from __future__ import annotations

import asyncio
import os
import random
import time
from collections.abc import AsyncGenerator, Iterable
from datetime import UTC, date, datetime
from typing import Any, Literal, Self, overload

import httpx

from ._stream import Stream
from ._version import USER_AGENT
from .errors import APIError, NetworkError, NotFound, RateLimited
from .types import (
    Devig,
    DropsResponse,
    Event,
    EventsResponse,
    History,
    HistoryMatch,
    LeaguesResponse,
    MarketType,
    MatchResponse,
    OddsFormat,
    OpeningClosingResponse,
    SearchResponse,
    SportsResponse,
    Timeline,
)

BASE_URL = "https://api.oddssea.dev"

DAY_MS = 86_400_000
MAX_IDS = 200                   # ids the API takes in one request
MAX_RANGE_MS = 31 * DAY_MS      # widest range of one /v1/history/events request
PAGE_SIZE = 500

# A sport is its id or its name: 29 and "soccer" are the same.
SportRef = int | str
Ids = int | Iterable[int]
When = int | str | date | datetime


class AsyncClient:
    """The oddssea API.

    The key comes from api_key or the ODDSSEA_API_KEY environment variable.
    Methods return the JSON the API sent, as described at https://oddssea.dev/docs.

    A request that fails to connect or gets a 502, 503 or 504 is tried again, up to
    max_retries times. So is a 429 when the API asks for a wait of at most
    max_retry_wait seconds. A longer wait raises RateLimited.
    """

    def __init__(self, api_key: str | None = None, *, base_url: str = BASE_URL, timeout: float = 30.0,
                 max_retries: int = 2, max_retry_wait: float = 5.0):
        api_key = api_key or os.environ.get("ODDSSEA_API_KEY")
        if not api_key:
            raise ValueError("no API key: pass api_key or set ODDSSEA_API_KEY")
        self.max_retries = max_retries
        self.max_retry_wait = max_retry_wait
        self.plan: str | None = None
        self.plan_expires: str | None = None
        self._base_url = base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key, "User-Agent": USER_AGENT}
        self._http = httpx.AsyncClient(timeout=timeout)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def sports(self) -> SportsResponse:
        """Every sport, with how many events it has now."""
        return await self._get("/v1/sports")

    async def leagues(self, sport: SportRef) -> LeaguesResponse:
        return await self._get("/v1/leagues", {"sport": sport})

    async def events(self, sport: SportRef, *, live: bool | None = None, leagues: Ids | None = None,
                     events: Ids | None = None, matches: Ids | None = None, since: int | None = None,
                     odds_format: OddsFormat | None = None, devig: Devig | None = None) -> EventsResponse:
        """A sport's events with their markets.

        live: True for in-play only, False for prematch only.
        since: the "last" of an earlier answer, to get only what changed after it.
        """
        return await self._get("/v1/events", {
            "sport": sport, "live": live, "league": _joined(leagues), "event": _joined(events),
            "match": _joined(matches), "since": since, "oddsFormat": odds_format, "devig": devig,
        })

    async def event(self, event_id: int, *, odds_format: OddsFormat | None = None,
                    devig: Devig | None = None) -> Event:
        return await self._get(f"/v1/events/{event_id}", {"oddsFormat": odds_format, "devig": devig})

    async def match(self, match_id: int, *, odds_format: OddsFormat | None = None,
                    devig: Devig | None = None) -> MatchResponse:
        """Every event of a match: in-play, prematch while it's still listed, and linked ones."""
        return await self._get(f"/v1/matches/{match_id}", {"oddsFormat": odds_format, "devig": devig})

    async def search(self, q: str | None = None, *, home: str | None = None, away: str | None = None,
                     sport: SportRef | None = None, starts: When | None = None, window: float | None = None,
                     live: bool | None = None, limit: int | None = None) -> SearchResponse:
        """Find listed matches by team or player names.

        starts with window keeps matches that start within that many hours of it.
        """
        return await self._get("/v1/search", {
            "q": q, "home": home, "away": away, "sport": sport,
            "starts": None if starts is None else _ms(starts),
            "window": window, "live": live, "limit": limit,
        })

    async def history(self, event: int, period: int, market: MarketType, line: float | None = None) -> History:
        """Every price and limit change of one line, and when it couldn't be bet."""
        return await self._get("/v1/history", {"event": event, "period": period, "market": market, "line": line})

    async def timeline(self, event: int) -> Timeline:
        """A match in play: its state, clock and score at each change, and when it was suspended."""
        return await self._get("/v1/history", {"event": event})

    async def history_events(self, sport: SportRef, *, from_: When | None = None, to: When | None = None,
                             leagues: int | str | Iterable[int | str] | None = None, q: str | None = None,
                             home: str | None = None, away: str | None = None) -> AsyncGenerator[HistoryMatch, None]:
        """Matches by start time, finished ones included, in start order.

        from_ and to take a date, a datetime, an ISO string or Unix ms. A date as
        to includes that day. Without them: the last 7 days.

        The API answers in pages and takes at most 31 days per request. This follows
        the pages and splits a longer range, so one loop covers any range.
        """
        end = int(time.time() * 1000) if to is None else _ms(to, end=True)
        start = end - 7 * DAY_MS if from_ is None else _ms(from_)
        while start < end:
            stop = min(start + MAX_RANGE_MS, end)
            cursor = None
            while True:
                # the API's "to" is inclusive
                page = await self._get("/v1/history/events", {
                    "sport": sport, "from": start, "to": stop - 1, "league": _listed(leagues),
                    "q": q, "home": home, "away": away, "limit": PAGE_SIZE, "cursor": cursor,
                })
                for match in page["matches"]:
                    yield match
                cursor = page["next"]
                if cursor is None:
                    break
            start = stop

    async def opening_closing(self, events: Ids, *, devig: Devig | None = None) -> OpeningClosingResponse:
        """Opening and closing prices of every line of the given events.

        Takes any number of ids. The API's limit of 200 per request is handled here.
        """
        ids = _listed(events)
        found = []
        for i in range(0, len(ids), MAX_IDS):
            answer = await self._get("/v1/opening-closing", {"event": _joined(ids[i:i + MAX_IDS]), "devig": devig})
            found += answer["events"]
        return {"events": found}

    async def drops(self, *, window: int | Literal["open"] | None = None,
                    direction: Literal["drop", "rise", "both", "limit"] | None = None,
                    min_drop: float | None = None, min_odds: float | None = None, max_odds: float | None = None,
                    min_max: float | None = None, sport: SportRef | Iterable[SportRef] | None = None,
                    live: bool | None = None, market: str | Iterable[str] | None = None,
                    main: bool | None = None) -> DropsResponse:
        """The biggest price moves right now, or with direction="limit" the limit changes."""
        return await self._get("/v1/drops", {
            "window": window, "direction": direction, "minDrop": min_drop, "minOdds": min_odds,
            "maxOdds": max_odds, "minMax": min_max, "sport": _listed(sport), "live": live,
            "market": _listed(market), "main": main,
        })

    def stream(self, sports: SportRef | Iterable[SportRef] | None = None, *, events: Ids | None = None,
               matches: Ids | None = None, live: bool = False, odds_format: OddsFormat | None = None,
               devig: Devig | None = None, opened: bool = False, limits: bool = False) -> Stream:
        """Subscribe to price changes over WebSocket. See Stream.

        Without sports, events or matches: everything the plan includes.
        live: in-play only. opened and limits turn on those messages.
        """
        subscription = {
            "op": "subscribe", "sports": _listed(sports), "events": _listed(events), "matches": _listed(matches),
            "live": live, "oddsFormat": odds_format, "devig": devig, "opened": opened, "limits": limits,
        }
        subscription = {name: value for name, value in subscription.items() if value is not None}
        url = "ws" + self._base_url.removeprefix("http") + "/v1/stream"
        return Stream(url, self._headers, subscription)

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = {name: _query_value(value) for name, value in (params or {}).items() if value is not None}
        attempt = 0
        while True:
            try:
                response = await self._http.get(self._base_url + path, params=query, headers=self._headers)
            except httpx.TransportError as exc:
                if attempt == self.max_retries:
                    raise NetworkError(str(exc) or type(exc).__name__) from exc
                wait = _backoff(attempt)
            else:
                if "X-Plan" in response.headers:
                    self.plan = response.headers["X-Plan"]
                    self.plan_expires = response.headers.get("X-Plan-Expires")
                if response.is_success:
                    return response.json()
                error = _error(response)
                retry_in = self._wait_before_retry(error, attempt)
                if retry_in is None or attempt == self.max_retries:
                    raise error
                wait = retry_in
            attempt += 1
            await asyncio.sleep(wait)

    def _wait_before_retry(self, error: APIError, attempt: int) -> float | None:
        if isinstance(error, RateLimited):
            return error.retry_after if error.retry_after <= self.max_retry_wait else None
        if error.status in (502, 503, 504):
            return _backoff(attempt)
        return None


def _error(response: httpx.Response) -> APIError:
    try:
        body = response.json()
        code, message = body["code"], body["message"]
    except (ValueError, KeyError, TypeError):
        # not from the API itself: a gateway's error page
        code, message = "ERROR", response.reason_phrase
    if response.status_code == 404:
        return NotFound(404, code, message)
    if response.status_code == 429:
        return RateLimited(429, code, message, float(response.headers.get("Retry-After", 1)))
    return APIError(response.status_code, code, message)


def _backoff(attempt: int) -> float:
    return 0.5 * 2 ** attempt * random.uniform(0.5, 1)


def _query_value(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    return value


@overload
def _listed(value: None) -> None: ...


@overload
def _listed(value: int | str | Iterable[Any]) -> list: ...


def _listed(value):
    """One value or several, as a list."""
    if value is None:
        return None
    if isinstance(value, (int, str)):
        return [value]
    return list(value)


def _joined(ids: Ids | None) -> str | None:
    if ids is None:
        return None
    return ",".join(str(i) for i in _listed(ids))


def _ms(when: When, end: bool = False) -> int:
    """Unix ms of a date, a datetime, an ISO string or Unix ms.

    A time without a zone is UTC. With end, the first ms after it: for a date,
    the start of the next day.
    """
    if isinstance(when, str):
        if when.isdigit():
            when = int(when)
        elif len(when) == 10:
            when = date.fromisoformat(when)
        else:
            when = datetime.fromisoformat(when)
    if isinstance(when, datetime):
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        return int(when.timestamp() * 1000) + (1 if end else 0)
    if isinstance(when, date):
        midnight = datetime(when.year, when.month, when.day, tzinfo=UTC)
        return int(midnight.timestamp() * 1000) + (DAY_MS if end else 0)
    return when + (1 if end else 0)
