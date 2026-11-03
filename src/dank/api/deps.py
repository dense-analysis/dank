"""Dependencies every feature shares. Features add their own in deps.py."""

from collections.abc import Callable
from typing import Annotated, get_origin

from fastapi import Depends, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel

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


def single_values(model: type[BaseModel]) -> Callable[[Request], None]:
    """Reject repeated keys for a query model's scalar fields.

    Starlette keeps a repeated key's last value, so Pydantic never sees the
    others. Only tuple fields may repeat.
    """
    repeatable = {
        name for name, field in model.model_fields.items()
        if get_origin(field.annotation) is tuple
    }

    def check(request: Request) -> None:
        params = request.query_params
        repeated = sorted(
            key for key in params.keys() - repeatable
            if len(params.getlist(key)) > 1
        )

        if repeated:
            raise RequestValidationError([
                {
                    "type": "repeated",
                    "loc": ("query", key),
                    "msg": f"{key} must appear only once",
                }
                for key in repeated
            ])

    return check
