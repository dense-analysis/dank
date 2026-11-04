"""The source icon route's query."""

from pydantic import ConfigDict

from dank.api.schemas import ApiModel


class SourceIconQuery(ApiModel):
    # This route has always ignored other keys, and treats a missing domain
    # like any other it has no icon for.
    model_config = ConfigDict(extra="ignore")

    domain: str = ""
