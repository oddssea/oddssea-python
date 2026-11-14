"""The shapes of the API's answers, for editors and type checkers.

At run time every answer is the plain dict the API sent. The API only ever adds
fields, so a dict can hold keys that aren't listed here.
"""

from typing import Any, Literal, NotRequired, TypedDict

OddsFormat = Literal["Decimal", "American", "HongKong", "Indonesian", "Malay"]
Devig = Literal["multiplicative", "additive", "power", "shin"]
MarketType = Literal["moneyline", "1x2", "handicap", "total"]

# Prices by side: home / away / draw, or over / under.
Prices = dict[str, float]

# A stream message. Its "type" says which keys it has (https://oddssea.dev/docs#messages).
Message = dict[str, Any]


class Sport(TypedDict):
    id: int
    sport: str
    name: str
    events: int
    live: int


class SportsResponse(TypedDict):
    sports: list[Sport]


class League(TypedDict):
    id: int
    name: str
    events: int


class LeaguesResponse(TypedDict):
    sport: str
    leagues: list[League]


class Market(TypedDict):
    market: MarketType
    period: int
    periodName: str
    status: Literal["open", "suspended"]
    line: float | None
    main: bool
    prices: Prices
    maxVolume: float
    maxStake: dict[str, int]
    updatedAt: NotRequired[str]
    fair: NotRequired[Prices]


class Event(TypedDict):
    id: int
    matchId: int
    sport: str
    sportId: int
    league: str
    leagueId: int
    home: str
    away: str
    starts: str
    live: bool
    state: str | None
    score: list[int] | None
    suspended: bool
    parentId: int | None
    unit: str
    markets: list[Market]


class RemovedEvent(TypedDict):
    id: int
    removed: Literal[True]


class EventsResponse(TypedDict):
    sport: str
    events: list[Event | RemovedEvent]
    # A free key gets delayed instead of last.
    last: NotRequired[int]
    delayed: NotRequired[bool]


class MatchResponse(TypedDict):
    matchId: int
    sport: str
    events: list[Event]


class SearchResult(TypedDict):
    score: float
    id: int
    matchId: int
    sport: str
    sportId: int
    league: str
    home: str
    away: str
    starts: str
    live: bool
    unit: str


class SearchResponse(TypedDict):
    matches: list[SearchResult]


class History(TypedDict):
    sides: list[str]
    points: list[list[float]]
    # [from, to, reason] in Unix ms: when the line couldn't be bet. reason is
    # "suspended" or "unlisted"; to is None while it still can't.
    unavailable: list[list[Any]] | None
    recordedFrom: int
    opening: Prices | None
    closing: Prices | None
    liveAt: int | None
    prematchEvent: int | None


class TimelineRow(TypedDict):
    t: int
    state: str | None
    phase: str | None
    clock: int | None
    score: list[int] | None
    # tennis only
    games: NotRequired[list[int]]
    points: NotRequired[list[str]]
    server: NotRequired[int]


class Timeline(TypedDict):
    event: int
    matchId: int
    liveAt: int | None
    recordedFrom: int
    timeline: list[TimelineRow]
    suspended: list[list[int | None]]


class PeriodResult(TypedDict):
    period: int
    periodName: str
    home: NotRequired[int]
    away: NotRequired[int]
    status: NotRequired[Literal["void"]]


class Result(TypedDict):
    status: Literal["final", "void"]
    winner: Literal["home", "away", "draw"] | None
    periods: list[PeriodResult]
    settledAt: int
    updatedAt: int


class LinkedEvent(TypedDict):
    id: int
    unit: str
    live: bool
    result: Result | None


class HistoryMatch(TypedDict):
    matchId: int
    id: int | None
    liveId: NotRequired[int | None]
    league: str
    leagueId: int | None
    home: str
    away: str
    starts: str
    kickoff: int | None
    closing: bool
    historyUntil: int
    linked: list[LinkedEvent]
    result: Result | None
    score: NotRequired[float]


class MainLines(TypedDict):
    opening: list[str]
    closing: list[str]


class FairLines(TypedDict):
    opening: dict[str, Prices] | None
    closing: dict[str, Prices] | None


class OpeningClosing(TypedDict):
    id: int
    opening: dict[str, Prices]
    closing: dict[str, Prices] | None
    main: MainLines
    kickoff: int | None
    final: bool
    fair: NotRequired[FairLines]


class OpeningClosingResponse(TypedDict):
    events: list[OpeningClosing]


class DropsResponse(TypedDict):
    drops: list[dict[str, Any]]
    now: int
