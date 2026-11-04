"""Which paths the built reader answers with its entry page."""

import pathlib


def client_route(path: str) -> bool:
    """Whether a path missing from the build is a route the app handles itself.

    Only extensionless paths inside the build are, so a missing bundle gets a
    404 rather than HTML. `path` is normalized, so any escape starts with "..".
    """
    route = pathlib.PurePosixPath(path)

    return not route.suffix and ".." not in route.parts
