"""In-memory stand-ins for the sources feature's driven adapters."""

from collections.abc import Mapping

from dank.api.sources.model import RedditActivity, XActivity
from dank.api.sources.service import CollectionStats


class FakeCollectionStats:
    """Answers from values a test sets. It records nothing: tests check
    the HTTP response, never which calls were made."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.blog_count = 0
        self.x: dict[str, XActivity] = {}
        self.reddit = RedditActivity(communities=(), flairs=())

    async def domain_counts(self) -> dict[str, int]:
        return self.counts

    async def external_blog_count(self) -> int:
        return self.blog_count

    async def x_activity(
        self, domain: str, accounts: tuple[str, ...],
    ) -> XActivity:
        return self.x.get(domain, XActivity(accounts=(), for_you_count=0))

    async def reddit_activity(
        self, communities: Mapping[str, str],
    ) -> RedditActivity:
        return self.reddit


# Pyright checks this assignment, so the fake can't drift from the port.
_PORT: type[CollectionStats] = FakeCollectionStats
