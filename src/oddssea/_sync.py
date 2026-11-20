from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine, Iterable, Iterator
from typing import Any, Literal, Self, TypeVar

from ._client import BASE_URL, AsyncClient, Ids, SportRef, When
from ._stream import Stream
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
    Message,
    OddsFormat,
    OpeningClosingResponse,
    SearchResponse,
    SportsResponse,
    Timeline,
)

T = TypeVar("T")


class Client:
    """The oddssea API, without asyncio.

    The same methods as AsyncClient, called without await. The work happens on
    an event loop in a background thread, which close() stops.
    """

    def __init__(self, api_key: str | None = None, *, base_url: str = BASE_URL, timeout: float = 30.0,
                 max_retries: int = 2, max_retry_wait: float = 5.0):
        self._async = AsyncClient(api_key, base_url=base_url, timeout=timeout, max_retries=max_retries,
                                  max_retry_wait=max_retry_wait)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="oddssea", daemon=True)
        self._thread.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        if self._loop.is_closed():
            return
        self._wait(self._async.aclose())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    @property
    def plan(self) -> str | None:
        return self._async.plan

    @property
    def plan_expires(self) -> str | None:
        return self._async.plan_expires

    def sports(self) -> SportsResponse:
        """Every sport, with how many events it has now."""
        return self._wait(self._async.sports())

    def leagues(self, sport: SportRef) -> LeaguesResponse:
        return self._wait(self._async.leagues(sport))

    def events(self, sport: SportRef, *, live: bool | None = None, leagues: Ids | None = None,
               events: Ids | None = None, matches: Ids | None = None, since: int | None = None,
               odds_format: OddsFormat | None = None, devig: Devig | None = None) -> EventsResponse:
        """A sport's events with their markets.

        live: True for in-play only, False for prematch only.
        since: the "last" of an earlier answer, to get only what changed after it.
        """
        return self._wait(self._async.events(sport, live=live, leagues=leagues, events=events, matches=matches,
                                             since=since, odds_format=odds_format, devig=devig))

    def event(self, event_id: int, *, odds_format: OddsFormat | None = None, devig: Devig | None = None) -> Event:
        return self._wait(self._async.event(event_id, odds_format=odds_format, devig=devig))

    def match(self, match_id: int, *, odds_format: OddsFormat | None = None,
              devig: Devig | None = None) -> MatchResponse:
        """Every event of a match: in-play, prematch while it's still listed, and linked ones."""
        return self._wait(self._async.match(match_id, odds_format=odds_format, devig=devig))

    def search(self, q: str | None = None, *, home: str | None = None, away: str | None = None,
               sport: SportRef | None = None, starts: When | None = None, window: float | None = None,
               live: bool | None = None, limit: int | None = None) -> SearchResponse:
        """Find listed matches by team or player names.

        starts with window keeps matches that start within that many hours of it.
        """
        return self._wait(self._async.search(q, home=home, away=away, sport=sport, starts=starts, window=window,
                                             live=live, limit=limit))

    def history(self, event: int, period: int, market: MarketType, line: float | None = None) -> History:
        """Every price and limit change of one line, and when it couldn't be bet."""
        return self._wait(self._async.history(event, period, market, line))

    def timeline(self, event: int) -> Timeline:
        """A match in play: its state, clock and score at each change, and when it was suspended."""
        return self._wait(self._async.timeline(event))

    def history_events(self, sport: SportRef, *, from_: When | None = None, to: When | None = None,
                       leagues: int | str | Iterable[int | str] | None = None, q: str | None = None,
                       home: str | None = None, away: str | None = None) -> Iterator[HistoryMatch]:
        """Matches by start time, finished ones included, in start order.

        from_ and to take a date, a datetime, an ISO string or Unix ms. A date as
        to includes that day. Without them: the last 7 days.

        The API answers in pages and takes at most 31 days per request. This follows
        the pages and splits a longer range, so one loop covers any range.
        """
        matches = self._async.history_events(sport, from_=from_, to=to, leagues=leagues, q=q, home=home, away=away)
        while True:
            try:
                yield self._wait(matches.__anext__())
            except StopAsyncIteration:
                return

    def opening_closing(self, events: Ids, *, devig: Devig | None = None) -> OpeningClosingResponse:
        """Opening and closing prices of every line of the given events.

        Takes any number of ids. The API's limit of 200 per request is handled here.
        """
        return self._wait(self._async.opening_closing(events, devig=devig))

    def drops(self, *, window: int | Literal["open"] | None = None,
              direction: Literal["drop", "rise", "both", "limit"] | None = None,
              min_drop: float | None = None, min_odds: float | None = None, max_odds: float | None = None,
              min_max: float | None = None, sport: SportRef | Iterable[SportRef] | None = None,
              live: bool | None = None, market: str | Iterable[str] | None = None,
              main: bool | None = None) -> DropsResponse:
        """The biggest price moves right now, or with direction="limit" the limit changes."""
        return self._wait(self._async.drops(window=window, direction=direction, min_drop=min_drop, min_odds=min_odds,
                                            max_odds=max_odds, min_max=min_max, sport=sport, live=live,
                                            market=market, main=main))

    def stream(self, sports: SportRef | Iterable[SportRef] | None = None, *, events: Ids | None = None,
               matches: Ids | None = None, live: bool = False, odds_format: OddsFormat | None = None,
               devig: Devig | None = None, opened: bool = False, limits: bool = False) -> SyncStream:
        """Subscribe to price changes over WebSocket. See Stream.

        Without sports, events or matches: everything the plan includes.
        live: in-play only. opened and limits turn on those messages.
        """
        return SyncStream(self._async.stream(sports, events=events, matches=matches, live=live,
                                             odds_format=odds_format, devig=devig, opened=opened, limits=limits),
                          self._wait)

    def _wait(self, coroutine: Coroutine[Any, Any, T]) -> T:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result()
        except KeyboardInterrupt:
            future.cancel()
            raise


class SyncStream:
    """Stream for a plain for loop. See Stream for the messages.

        with client.stream("soccer") as stream:
            for message in stream:
                ...
    """

    def __init__(self, stream: Stream, wait: Callable[[Coroutine[Any, Any, Any]], Any]):
        self._stream = stream
        self._wait = wait

    @property
    def events(self) -> dict[int, Event]:
        return self._stream.events

    @property
    def connected(self) -> bool:
        return self._stream.connected

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Message:
        try:
            return self._wait(self._stream.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self._wait(self._stream.close())
