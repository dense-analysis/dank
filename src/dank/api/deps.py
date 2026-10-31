"""Dependencies every feature shares. Features add their own in deps.py."""

from typing import Annotated

from fastapi import Depends, Request

from dank.api.lifespan import Resources
from dank.config import Settings


def resources(request: Request) -> Resources:
    # The lifespan yields {"resources": ...} and Starlette exposes it here.
    # Without the lifespan this raises, so the route answers 503 and a
    # test that forgot an override fails loudly.
    return request.state.resources


ResourcesDep = Annotated[Resources, Depends(resources)]


def settings(resources: ResourcesDep) -> Settings:
    return resources.settings


SettingsDep = Annotated[Settings, Depends(settings)]
