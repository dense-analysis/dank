"""One error shape for the API: {"error", "code", "fields"}."""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException

from dank.api.schemas import ErrorBody, FieldError
from dank.errors import Conflict, DankError, InvalidInput, NotFound

logger = logging.getLogger(__name__)

STATUS_BY_ERROR: Mapping[type[DankError], int] = {
    InvalidInput: 400,
    NotFound: 404,
    Conflict: 409,
}
CODE_BY_STATUS: Mapping[int, str] = {
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    415: "unsupported_media_type",
}
# Today's wording, which the reader shows as is.
UNAVAILABLE = "The collection is unavailable. Please try again."


async def domain_error(request: Request, error: DankError) -> JSONResponse:
    status = next(
        (
            status for kind, status in STATUS_BY_ERROR.items()
            if isinstance(error, kind)
        ),
        500,
    )

    return envelope(status, str(error), error.code)


async def http_error(request: Request, error: HTTPException) -> JSONResponse:
    code = CODE_BY_STATUS.get(error.status_code, "http_error")

    return envelope(
        error.status_code, str(error.detail), code, headers=error.headers,
    )


async def validation_error(
    request: Request, error: RequestValidationError,
) -> JSONResponse:
    fields = tuple(
        FieldError(
            path=".".join(str(part) for part in item["loc"]),
            code=item["type"],
            message=item["msg"],
        )
        for item in error.errors()
    )

    # 400 rather than FastAPI's 422, as today; DEN-53 may revisit it.
    return envelope(400, "The request is invalid.", "invalid_request", fields)


async def unexpected_error(
    request: Request, error: Exception,
) -> JSONResponse:
    logger.error("Request failed: %s", request.url.path, exc_info=error)

    # Starlette runs this handler outside every middleware, so ApiHeaders
    # can't add the header that keeps a failure out of the browser's cache.
    return envelope(
        503, UNAVAILABLE, "unavailable", headers={"cache-control": "no-store"},
    )


type ExceptionHandler = Callable[
    [Request, Any], Coroutine[Any, Any, Response],
]

EXCEPTION_HANDLERS: dict[int | type[Exception], ExceptionHandler] = {
    DankError: domain_error,
    HTTPException: http_error,
    RequestValidationError: validation_error,
    Exception: unexpected_error,
}


def envelope(
    status: int,
    message: str,
    code: str,
    fields: Sequence[FieldError] = (),
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorBody(error=message, code=code, fields=tuple(fields))

    return JSONResponse(
        body.model_dump(), status_code=status, headers=headers,
    )
