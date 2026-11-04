"""The legacy viewer's queries. Its pages have no response models."""

from pydantic import ConfigDict

from dank.api.schemas import ApiModel
from dank.api.viewer.model import IndexFilters, index_filters


class IndexQuery(ApiModel):
    # Text, because the page has always forgiven whatever it can't read, and
    # model.py decides what that is. Other keys have always been ignored.
    model_config = ConfigDict(extra="ignore")

    limit: str | None = None
    q: str | None = None
    domain: str | None = None
    account: str | None = None
    days_back: str | None = None
    cursor_created_at: str | None = None
    cursor_post_id: str | None = None
    cursor_direction: str | None = None

    def filters(self, page_size: int) -> IndexFilters:
        return index_filters(self.model_dump(), page_size=page_size)


class PostQuery(ApiModel):
    # The service says what's missing, in the wording the page has always used.
    model_config = ConfigDict(extra="ignore")

    domain: str | None = None
    post_id: str | None = None
