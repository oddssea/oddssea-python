class OddsseaError(Exception):
    """Base class of this package's errors."""


class NetworkError(OddsseaError):
    """The API couldn't be reached."""


class APIError(OddsseaError):
    """The API answered with an error.

    code and message are the API's own (https://oddssea.dev/docs#errors).
    status is the HTTP status, or None for an error sent on the stream.
    """

    def __init__(self, status: int | None, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.status = status
        self.code = code
        self.message = message


class NotFound(APIError):
    """No such sport, event or match."""


class RateLimited(APIError):
    """Over a request limit. retry_after is how many seconds the API asks you to wait."""

    def __init__(self, status: int, code: str, message: str, retry_after: float):
        super().__init__(status, code, message)
        self.retry_after = retry_after
