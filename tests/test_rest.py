import asyncio
from datetime import UTC, date, datetime, timedelta, timezone

import httpx
import pytest

import oddssea
from oddssea import _client

DAY = 86_400_000


def client_for(handler, **options) -> oddssea.AsyncClient:
    """A client whose requests go to handler instead of the network."""
    client = oddssea.AsyncClient("test-key", **options)
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def answers(*responses):
    """A handler that gives the responses in turn and keeps the requests it got."""
    queue = list(responses)
    requests = []

    def handler(request):
        requests.append(request)
        return queue.pop(0)

    handler.requests = requests
    return handler


def error(status, code, **headers):
    return httpx.Response(status, json={"code": code, "message": "no"}, headers=headers)


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    """Retries wait through asyncio.sleep: record the waits instead."""
    waits = []

    async def sleep(seconds):
        waits.append(seconds)

    monkeypatch.setattr(_client.asyncio, "sleep", sleep)
    return waits


def test_no_key(monkeypatch):
    monkeypatch.delenv("ODDSSEA_API_KEY", raising=False)
    with pytest.raises(ValueError):
        oddssea.AsyncClient()


def test_key_from_environment(monkeypatch):
    monkeypatch.setenv("ODDSSEA_API_KEY", "from-env")
    assert oddssea.AsyncClient()._headers["X-API-Key"] == "from-env"


def test_events_request():
    handler = answers(httpx.Response(200, json={"sport": "soccer", "last": 5, "events": []}))
    client = client_for(handler)

    answer = asyncio.run(client.events("soccer", live=True, leagues=[1980, 2157], matches=7, odds_format="American"))

    assert answer == {"sport": "soccer", "last": 5, "events": []}
    request = handler.requests[0]
    assert request.url.path == "/v1/events"
    assert dict(request.url.params) == {
        "sport": "soccer", "live": "1", "league": "1980,2157", "match": "7", "oddsFormat": "American",
    }
    assert request.headers["X-API-Key"] == "test-key"


def test_plan_headers():
    handler = answers(httpx.Response(200, json={"sports": []}, headers={
        "X-Plan": "live", "X-Plan-Expires": "2026-12-02T10:00:00Z",
    }))
    client = client_for(handler)

    asyncio.run(client.sports())

    assert client.plan == "live"
    assert client.plan_expires == "2026-12-02T10:00:00Z"


def test_timeline_is_history_without_a_line():
    handler = answers(httpx.Response(200, json={"event": 7, "timeline": [], "suspended": []}))
    client = client_for(handler)

    asyncio.run(client.timeline(7))

    request = handler.requests[0]
    assert request.url.path == "/v1/history"
    assert dict(request.url.params) == {"event": "7"}


def test_not_found():
    client = client_for(answers(error(404, "NOT_FOUND")))

    with pytest.raises(oddssea.NotFound) as caught:
        asyncio.run(client.event(1))

    assert caught.value.status == 404
    assert caught.value.code == "NOT_FOUND"


def test_short_rate_limit_is_waited_out(no_waiting):
    handler = answers(error(429, "TOO_MANY_REQUESTS", **{"Retry-After": "2"}), httpx.Response(200, json={"sports": []}))
    client = client_for(handler)

    assert asyncio.run(client.sports()) == {"sports": []}
    assert no_waiting == [2.0]


def test_long_rate_limit_raises(no_waiting):
    client = client_for(answers(error(429, "TOO_MANY_REQUESTS", **{"Retry-After": "60"})))

    with pytest.raises(oddssea.RateLimited) as caught:
        asyncio.run(client.sports())

    assert caught.value.retry_after == 60
    assert no_waiting == []


def test_rate_limit_waiting_turned_off():
    client = client_for(answers(error(429, "TOO_MANY_REQUESTS", **{"Retry-After": "1"})), max_retry_wait=0)

    with pytest.raises(oddssea.RateLimited):
        asyncio.run(client.sports())


def test_unavailable_is_retried():
    handler = answers(
        error(503, "SERVICE_UNAVAILABLE"),
        httpx.Response(502, text="Bad Gateway"),
        httpx.Response(200, json={"sports": []}),
    )
    client = client_for(handler)

    assert asyncio.run(client.sports()) == {"sports": []}
    assert len(handler.requests) == 3


def test_retries_run_out():
    handler = answers(*[error(503, "SERVICE_UNAVAILABLE")] * 3)
    client = client_for(handler)

    with pytest.raises(oddssea.APIError) as caught:
        asyncio.run(client.sports())

    assert caught.value.status == 503
    assert len(handler.requests) == 3


def test_connection_failure():
    def handler(request):
        raise httpx.ConnectError("refused")

    client = client_for(handler)

    with pytest.raises(oddssea.NetworkError):
        asyncio.run(client.sports())


def test_history_events_follows_pages():
    handler = answers(
        httpx.Response(200, json={"matches": [{"matchId": 1}, {"matchId": 2}], "next": "10.2"}),
        httpx.Response(200, json={"matches": [{"matchId": 3}], "next": None}),
    )
    client = client_for(handler)

    async def collect():
        return [m["matchId"] async for m in client.history_events("soccer", from_="2026-10-05", to="2026-10-11")]

    assert asyncio.run(collect()) == [1, 2, 3]
    first, second = (dict(r.url.params) for r in handler.requests)
    assert "cursor" not in first
    assert second["cursor"] == "10.2"


def test_history_events_splits_long_ranges():
    handler = answers(*[httpx.Response(200, json={"matches": [], "next": None})] * 3)
    client = client_for(handler)

    async def collect():
        return [m async for m in client.history_events("soccer", from_=date(2026, 10, 1), to=date(2026, 12, 15))]

    asyncio.run(collect())

    ranges = [(int(r.url.params["from"]), int(r.url.params["to"])) for r in handler.requests]
    start = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp() * 1000)
    end = int(datetime(2026, 12, 16, tzinfo=UTC).timestamp() * 1000)
    assert ranges == [
        (start, start + 31 * DAY - 1),
        (start + 31 * DAY, start + 62 * DAY - 1),
        (start + 62 * DAY, end - 1),
    ]


def test_opening_closing_sends_200_ids_at_a_time():
    handler = answers(
        httpx.Response(200, json={"events": [{"id": 1}]}),
        httpx.Response(200, json={"events": [{"id": 2}]}),
    )
    client = client_for(handler)

    answer = asyncio.run(client.opening_closing(range(250), devig="power"))

    assert answer == {"events": [{"id": 1}, {"id": 2}]}
    sizes = [len(r.url.params["event"].split(",")) for r in handler.requests]
    assert sizes == [200, 50]


def test_drops_repeats_list_parameters():
    handler = answers(httpx.Response(200, json={"drops": [], "now": 1}))
    client = client_for(handler)

    asyncio.run(client.drops(window="open", sport=["soccer", 33], main=False))

    params = handler.requests[0].url.params
    assert params.get_list("sport") == ["soccer", "33"]
    assert params["main"] == "0"


@pytest.mark.parametrize("when, end, expected", [
    (1790000000000, False, 1790000000000),
    (1790000000000, True, 1790000000001),
    ("1790000000000", False, 1790000000000),
    (date(2026, 10, 5), False, datetime(2026, 10, 5, tzinfo=UTC)),
    (date(2026, 10, 5), True, datetime(2026, 10, 6, tzinfo=UTC)),
    ("2026-10-05", True, datetime(2026, 10, 6, tzinfo=UTC)),
    ("2026-10-05T12:00:00Z", False, datetime(2026, 10, 5, 12, tzinfo=UTC)),
    (datetime(2026, 10, 5, 12), False, datetime(2026, 10, 5, 12, tzinfo=UTC)),
    (datetime(2026, 10, 5, 14, tzinfo=timezone(timedelta(hours=2))), False, datetime(2026, 10, 5, 12, tzinfo=UTC)),
])
def test_times(when, end, expected):
    if isinstance(expected, datetime):
        expected = int(expected.timestamp() * 1000)
    assert _client._ms(when, end=end) == expected


def test_sync_client():
    handler = answers(
        httpx.Response(200, json={"sports": [{"id": 29}]}, headers={"X-Plan": "prematch"}),
        httpx.Response(200, json={"matches": [{"matchId": 1}], "next": None}),
    )
    with oddssea.Client("test-key") as client:
        client._async._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        assert client.sports() == {"sports": [{"id": 29}]}
        assert client.plan == "prematch"
        assert [m["matchId"] for m in client.history_events("soccer", from_="2026-10-05", to="2026-10-06")] == [1]


def test_line_key():
    assert oddssea.line_key(0, "1x2") == "0|1x2|"
    assert oddssea.line_key(0, "total", 2.5) == "0|total|2.5"
    assert oddssea.line_key(1, "handicap", -0.25) == "1|handicap|-0.25"
    assert oddssea.line_key(0, "total", 3.0) == "0|total|3"
    assert oddssea.line_key(0, "handicap", -0.0) == "0|handicap|0"
