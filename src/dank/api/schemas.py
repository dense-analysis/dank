"""Wire models every feature shares."""

from pydantic import BaseModel, ConfigDict


class ApiModel(BaseModel):
    """Base for every request and response model."""

    # Unknown keys are errors, models are immutable values, and domain
    # NamedTuples can be read by attribute.
    model_config = ConfigDict(
        extra="forbid", frozen=True, from_attributes=True,
    )


class FieldError(ApiModel):
    path: str
    code: str
    message: str


class ErrorBody(ApiModel):
    error: str
    code: str
    fields: tuple[FieldError, ...] = ()
