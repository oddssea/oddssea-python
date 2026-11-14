"""Python client for the oddssea API (https://oddssea.dev/docs)."""

from ._client import AsyncClient
from ._stream import Stream
from ._sync import Client, SyncStream
from ._version import __version__
from .errors import APIError, NetworkError, NotFound, OddsseaError, RateLimited

__all__ = [
    "APIError",
    "AsyncClient",
    "Client",
    "NetworkError",
    "NotFound",
    "OddsseaError",
    "RateLimited",
    "Stream",
    "SyncStream",
    "__version__",
    "line_key",
]


def line_key(period: int, market: str, line: float | None = None) -> str:
    """The key of a line in history and opening / closing answers.

    >>> line_key(0, "total", 2.5)
    '0|total|2.5'
    >>> line_key(0, "1x2")
    '0|1x2|'
    """
    if line is None:
        return f"{period}|{market}|"
    return f"{period}|{market}|{line + 0.0:g}"      # + 0.0 turns -0.0 into 0
