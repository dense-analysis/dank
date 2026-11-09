"""Which paths the built reader answers, and where its old addresses went."""

import pathlib

# The server's own paths, which keep their 404s rather than loading the app.
SERVER_PREFIXES = ("api", "assets")


def server_path(path: str) -> bool:
    """Whether a path belongs to the server rather than to the reader."""
    parts = pathlib.PurePosixPath(path).parts

    return bool(parts) and parts[0] in SERVER_PREFIXES


def client_route(path: str) -> bool:
    """Whether a path missing from the build is a route the app handles itself.

    Only extensionless paths inside the build are, so a missing bundle gets a
    404 rather than HTML. `path` is normalized, so any escape starts with "..".
    """
    route = pathlib.PurePosixPath(path)

    return (
        not route.suffix and ".." not in route.parts and not server_path(path)
    )


def reader_location(path: str, query: str) -> str:
    """Where an old /reader/ address lives now: its path and query at /."""
    location = "/" + path

    return f"{location}?{query}" if query else location
