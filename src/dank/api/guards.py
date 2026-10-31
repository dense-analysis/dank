"""One rule for unsafe methods: same origin, and a JSON body."""

from fastapi import HTTPException, Request

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


async def same_origin_json(request: Request) -> None:
    if request.method in SAFE_METHODS:
        return

    if not same_origin(request):
        raise HTTPException(403, "Same-origin request required")

    content_type = request.headers.get("content-type", "")

    if content_type.split(";")[0].strip() != "application/json":
        raise HTTPException(415, "JSON request required")


def same_origin(request: Request) -> bool:
    # The local HTTPS proxy rejects foreign origins itself, then rewrites
    # "https:" to "http:" for this server. So compare with the origin this
    # server sees, and run uvicorn with proxy_headers=False so a forwarded
    # scheme can't change it.
    origin = request.headers.get("origin")
    own = f"{request.url.scheme}://{request.headers.get('host')}"

    return (
        request.headers.get("sec-fetch-site") != "cross-site"
        and origin in (None, own)
    )
