# oddssea for Python

Python client for the [oddssea](https://oddssea.dev) API: Pinnacle odds, prematch and live, over REST and WebSocket.

## Install

```
pip install oddssea
```

Python 3.11 or newer. A free API key takes a minute at [app.oddssea.dev/account](https://app.oddssea.dev/account), no email needed.

## Quick start

```python
import oddssea

client = oddssea.Client("your-key")

for event in client.events("soccer")["events"]:
    print(event["starts"], event["home"], "-", event["away"])
```

```
2026-11-14T17:30:00Z CE Europa - Sant Andreu
2026-11-14T17:45:00Z Coria - Unionistas de Salamanca
...
```

This runs on a free key, with delayed prematch odds. The rest of the API needs a paid plan.

## Odds

Each event has its odds in `event["markets"]`, one entry per line on offer.

```python
{"market": "1x2", "period": 0, "periodName": "Match", "line": None,
 "prices": {"home": 1.645, "draw": 3.89, "away": 6.27},
 "maxStake": {"home": 1198, "draw": 775, "away": 775}, ...}
```

| Field | Meaning |
|---|---|
| `market` | `moneyline`, `1x2`, `handicap` or `total` |
| `period` | 0 is the whole match. `periodName` names the others (1st Half, Set 2) |
| `line` | the handicap or the total, `None` for moneylines |
| `prices` | decimal odds per side |
| `maxStake` | the limit per side, in USD |

Every answer is the JSON from the [API docs](https://oddssea.dev/docs), in plain dicts with the same field names.

## Methods

| Method | Gives you |
|---|---|
| `sports()` | every sport, with how many events it has |
| `leagues(sport)` | a sport's leagues |
| `events(sport, ...)` | a sport's events with their odds |
| `event(event_id)` | one event |
| `match(match_id)` | every event of a match, before and after kick-off |
| `search(q, home=, away=, ...)` | matches by team or player names |
| `history(event, period, market, line)` | every price change of one line |
| `timeline(event)` | a match in play: score, clock and suspensions |
| `history_events(sport, from_=, to=, ...)` | finished matches by date, with results |
| `opening_closing(events)` | opening and closing prices |
| `drops(...)` | the biggest price moves right now |
| `stream(sports, ...)` | changes as they happen |

`...` stands for optional arguments. They are the endpoint's parameters from the API docs with Python names, so `oddsFormat` is `odds_format`.

`Client` and `AsyncClient` have the same methods. The key can also come from the `ODDSSEA_API_KEY` environment variable.

## Stream

Prices, limits, suspensions and in-play scores, pushed as they change. On any paid plan.

```python
import asyncio
import oddssea

async def main():
    client = oddssea.AsyncClient("your-key")
    async for message in client.stream("soccer", live=True):
        if message["type"] == "update":
            event = message["event"]
            print(event["home"], event["score"], event["away"], event["state"])

asyncio.run(main())
```

```
Sheffield United [0, 3] Bristol City 2H 28'
Dordrecht [0, 0] Heracles Almelo 1H 30'
...
```

| Message | When |
|---|---|
| `snapshot` | once per event when you connect |
| `ready` | the snapshot is complete |
| `update` | an event changed. It carries the whole event |
| `remove` | an event is gone |
| `disconnect` | the connection was lost. Prices may be stale until the next `ready` |

`stream.events` always holds the current events by id.

The stream reconnects by itself and then sends what changed while it was away. When we update the service, it moves to a new connection without a gap.

Without asyncio, `oddssea.Client` has the same stream as a plain `for` loop.

## Closing prices

```python
matches = client.history_events("soccer", from_="2026-11-01", to="2026-11-07")
ids = [match["id"] for match in matches if match["id"]]

for event in client.opening_closing(ids)["events"]:
    if event["closing"]:
        print(event["id"], event["closing"].get("0|1x2|"))
```

```
1637370516 {'home': 1.96, 'draw': 3.55, 'away': 3.7}
...
```

Lines are keyed by period, market and line, like `"0|1x2|"` or `"0|total|2.5"`. `oddssea.line_key(0, "total", 2.5)` builds one.

Both methods take any date range and any number of ids.

## Match timeline

```python
client.timeline(1637600069)
```

```python
{"timeline": [
   {"t": 1791001800000, "state": "1H 0'",  "phase": "1H", "clock": 0,    "score": [0, 0]},
   {"t": 1791003386000, "state": "1H 26'", "phase": "1H", "clock": 26,   "score": [1, 0]},
   {"t": 1791004500000, "state": "HT",     "phase": "HT", "clock": None, "score": [1, 0]}],
 "suspended": [[1791003320000, 1791003386000]], ...}
```

A row each time the score, the clock or the state changed, and in `suspended` when the whole match was closed for betting. Here it closed for 66 seconds and reopened at 1-0.

The times are on the same clock as the prices from `history()`, so the score behind any price is the last row at or before it.

Needs a Live or Scale key.

## Errors

```python
try:
    client.event(123)
except oddssea.NotFound:
    ...
except oddssea.RateLimited as error:
    print("try again in", error.retry_after, "seconds")
```

| Error | Meaning |
|---|---|
| `APIError` | the API refused the request. Has `status`, `code` and `message` |
| `NotFound` | no such sport, event or match |
| `RateLimited` | over a request limit. Has `retry_after` |
| `NetworkError` | the API couldn't be reached |

A failed connection or a 502, 503 or 504 is tried again twice. A 429 is waited out when the API asks for 5 seconds or less.

## License

MIT, see [LICENSE](LICENSE). The license covers this code. Use of the data is covered by the [terms](https://oddssea.dev/terms).
