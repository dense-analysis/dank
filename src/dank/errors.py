"""Errors any layer may raise.

Each has a stable code for API clients. HTTP statuses are chosen at the
edge, in dank.api.errors, so services never import FastAPI.
"""


class DankError(Exception):
    code = "error"


class InvalidInput(DankError, ValueError):
    code = "invalid_input"


class NotFound(DankError):
    code = "not_found"


class Conflict(DankError):
    code = "conflict"


class Busy(DankError):
    """Bounded work ran out of time. The same request may work shortly."""

    code = "busy"


class TooManyRequests(DankError):
    code = "too_many_requests"
