from __future__ import annotations

import datetime
import json
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from dank.config import BrowserScrapeSettings, RedditSettings
from dank.scrape.reddit import scrape_reddit_communities
from dank.scrape.reddit.communities import discover_community
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import NetworkResponse

COMMUNITY = "LocalLLaMA"
LISTING_URL = f"https://www.reddit.com/r/{COMMUNITY}/top/?t=day"
# Minimal header elements observed in Reddit's public community documents.
ICON_URL = (
    "https://styles.redditmedia.com/t5_81eyvm/styles/"
    "communityIcon_cumnsvx9kzma1.png?width=128&frame=1&auto=webp"
)


def _header(
    community: str = COMMUNITY, community_id: str = "t5_81eyvm",
) -> str:
    return (
        f'<shreddit-subreddit-header-buttons name="{community}" '
        f'subreddit-id="{community_id}" prefixed-name="r/{community}">'
        "</shreddit-subreddit-header-buttons>"
    )


def _icon(url: str = ICON_URL) -> str:
    return (
        '<span id="subreddit-icon-img-desktop">'
        f'<img class="shreddit-subreddit-icon__icon" '
        f'src="{url.replace("&", "&amp;")}" alt=""></span>'
    )


def _document(
    body: str,
    *,
    url: str = LISTING_URL,
    status: int = 200,
) -> NetworkResponse:
    return NetworkResponse(url, status, "text/html", body, "req", "Document")


def _post(community: str = COMMUNITY) -> str:
    return (
        '<shreddit-post id="t3_abc" post-title="Public story" '
        f'permalink="/r/{community}/comments/abc/story/">'
        "</shreddit-post>"
    )


def test_document_discovers_matching_header_icon_and_minimal_original() -> (
    None
):
    source = "<html>" + _header() + _icon() + _post() + "</html>"
    discovery = discover_community([_document(source)], "localllama")

    assert discovery is not None
    assert discovery.community == COMMUNITY
    assert discovery.icon_url == ICON_URL
    assert discovery.request_url == LISTING_URL
    assert discovery.scraped_at.tzinfo is datetime.UTC
    evidence = json.loads(discovery.payload)["html"]
    assert '<shreddit-subreddit-header-buttons name="LocalLLaMA"' in evidence
    assert "&amp;frame=1" in evidence
    assert "shreddit-post" not in evidence
    assert "<html>" not in evidence


def test_dom_falls_back_when_document_is_missing_or_has_no_icon() -> None:
    discovery = discover_community(
        [_document(_header())],
        COMMUNITY,
        dom_html=_header() + _icon(),
        request_url=LISTING_URL,
    )

    assert discovery is not None
    assert discovery.icon_url == ICON_URL


@pytest.mark.parametrize(
    "wrapper",
    [
        "<shreddit-post>{}</shreddit-post>",
        "<shreddit-ad-post>{}</shreddit-ad-post>",
        '<div data-promoted="true">{}</div>',
        '<div is-promoted=""><br/><img src="x"/>{}</div>',
        '<div is-ad="true">{}</div>',
    ],
)
def test_post_and_promoted_icons_cannot_replace_header_icon(
    wrapper: str,
) -> None:
    wrong = ICON_URL.replace("cumnsvx9kzma1", "wrong")
    source = _header() + wrapper.format(_icon(wrong)) + _icon()
    discovery = discover_community([_document(source)], COMMUNITY)

    assert discovery is not None
    assert discovery.icon_url == ICON_URL
    assert "wrong" not in discovery.payload


def test_author_post_and_other_community_images_are_not_community_icons() -> (
    None
):
    source = (
        _header("accelerate", "t5_2x21z")
        + _icon(ICON_URL.replace("t5_81eyvm", "t5_2x21z"))
        + _header()
        + _icon(ICON_URL.replace("communityIcon_", "profileIcon_"))
        + f'<img src="{ICON_URL}" alt="Post image">'
        + _icon()
    )
    discovery = discover_community([_document(source)], COMMUNITY)

    assert discovery is not None
    assert discovery.icon_url == ICON_URL
    assert "accelerate" not in discovery.payload
    assert "profileIcon" not in discovery.payload


def test_same_icon_path_without_header_identity_is_not_enough() -> None:
    assert discover_community([_document(_icon())], COMMUNITY) is None
    assert (
        discover_community(
            [_document(_header("accelerate") + _icon())],
            COMMUNITY,
        )
        is None
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://[",
        "https://styles.redditmedia.com:bad/t5_81eyvm/styles/communityIcon_a.png",
        "https://styles.redditmedia.com:444/t5_81eyvm/styles/communityIcon_a.png",
        "http://styles.redditmedia.com/t5_81eyvm/styles/communityIcon_a.png",
        "https://styles.redditmedia.com.evil.test/t5_81eyvm/styles/communityIcon_a",
        "https://user@styles.redditmedia.com/t5_81eyvm/styles/communityIcon_a.png",
        "https://styles.redditmedia.com/t5_wrong/styles/communityIcon_a.png",
        "https://i.redd.it/post.png",
        "https://styles.redditmedia.com/t5_81eyvm/styles/profileIcon_a.png",
        "javascript:alert(1)",
        "/t5_81eyvm/styles/communityIcon_a.png",
    ],
)
def test_unsafe_or_unrelated_icon_urls_leave_valid_community_without_icon(
    url: str,
) -> None:
    discovery = discover_community(
        [_document(_header() + _icon(url))], COMMUNITY,
    )

    assert discovery is not None
    assert discovery.icon_url == ""
    assert "<img" not in discovery.payload


@pytest.mark.parametrize(
    "url",
    [
        "https://[",
        "https://www.reddit.com:bad/r/LocalLLaMA/top/",
        "https://www.reddit.com/r/accelerate/top/",
        "https://www.reddit.com/r/LocalLLaMA/comments/abc/story/",
        "https://evil.test/r/LocalLLaMA/top/",
        "https://user@www.reddit.com/r/LocalLLaMA/top/",
        "http://www.reddit.com/r/LocalLLaMA/top/",
    ],
)
def test_mismatched_or_invalid_listing_origin_is_rejected(url: str) -> None:
    assert (
        discover_community(
            [_document(_header() + _icon(), url=url)],
            COMMUNITY,
        )
        is None
    )


def test_unsuccessful_document_uses_valid_dom_instead() -> None:
    discovery = discover_community(
        [_document(_header() + _icon(), status=403)],
        COMMUNITY,
        dom_html=_header("localllama") + _icon(),
        request_url=LISTING_URL,
    )

    assert discovery is not None
    assert discovery.community == "localllama"


class _Page:
    def __init__(
        self, *, posts: bool = True, header: bool = True, icon: bool = True,
        icon_after_scroll: int | None = None,
    ) -> None:
        self.posts = posts
        self.header = header
        self.icon = icon
        self.icon_after_scroll = icon_after_scroll
        self.scrolls = 0
        self.community = COMMUNITY
        self.url = LISTING_URL
        self.visited: list[str] = []

    async def activate(self) -> None:
        pass

    async def send(
        self,
        command: Generator[dict[str, object], object, object],
    ) -> None:
        parameters = cast(dict[str, str], next(command)["params"])
        self.url = parameters["url"]
        self.community = self.url.split("/r/")[1].split("/")[0]
        self.visited.append(self.community)
        self.scrolls = 0

    async def evaluate(self, script: str) -> str:
        if script.startswith("JSON.stringify"):
            show_icon = self.icon or (
                self.icon_after_scroll is not None
                and self.scrolls >= self.icon_after_scroll
            )

            return json.dumps(
                {
                    "url": self.url,
                    "text": "A public community",
                    "html": _post(self.community) if self.posts else "",
                    "community_html": (
                        _header(self.community)
                        + (_icon() if show_icon else "")
                        if self.header
                        else ""
                    ),
                },
            )

        self.scrolls += 1

        return ""

    async def close(self) -> None:
        pass


async def _collect(
    page: _Page, communities: tuple[str, ...],
) -> list[ScrapeBatch]:
    session = AsyncMock()
    session.get_browser.return_value.get.return_value = page
    capture = AsyncMock()
    capture.drain.return_value = []

    with (
        patch("dank.scrape.reddit.NetworkCapture", return_value=capture),
        patch("dank.scrape.reddit.asyncio.sleep"),
    ):
        batches = [
            batch
            async for batch in scrape_reddit_communities(
                RedditSettings(
                    scrape=BrowserScrapeSettings(max_posts=10, max_scrolls=3),
                    stall_scrolls=2,
                ),
                communities,
                cast(Any, session),
            )
        ]

    return batches


async def test_metadata_deduplicates_within_each_run() -> None:
    page = _Page()

    for _ in range(2):
        batches = await _collect(page, (COMMUNITY, "localllama", "accelerate"))
        discoveries = [item for batch in batches for item in batch.communities]
        assert [item.community for item in discoveries] == [
            COMMUNITY,
            "accelerate",
        ]
        assert len([post for batch in batches for post in batch.posts]) == 2

    assert page.visited == [COMMUNITY, "accelerate", COMMUNITY, "accelerate"]


async def test_next_community_is_activated_after_detail_visits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FocusedPage(_Page):
        active = False

        async def activate(self) -> None:
            self.active = True

        async def send(self, command: Any) -> None:
            assert self.active, "Background navigation did not hydrate"
            await super().send(command)

    page = _FocusedPage()

    async def visit_details(batch: ScrapeBatch, *args: Any) -> ScrapeBatch:
        page.active = False
        return batch

    monkeypatch.setattr(
        "dank.scrape.reddit._capture_batch_details", visit_details,
    )
    batches = await _collect(page, (COMMUNITY, "accelerate"))
    assert page.visited == [COMMUNITY, "accelerate"]
    assert len([post for batch in batches for post in batch.posts]) == 2


@pytest.mark.parametrize("icon", [True, False])
async def test_public_empty_community_still_emits_metadata(
    *, icon: bool,
) -> None:
    batches = await _collect(_Page(posts=False, icon=icon), (COMMUNITY,))

    assert len(batches) == 1
    assert batches[0].posts == []
    assert batches[0].assets == []
    assert batches[0].communities[0].icon_url == (ICON_URL if icon else "")


async def test_missing_header_does_not_lose_posts() -> None:
    batches = await _collect(_Page(header=False), (COMMUNITY,))

    assert len(batches) == 1
    assert len(batches[0].posts) == 1
    assert batches[0].communities == ()


async def test_missing_icon_keeps_posts_and_community_identity() -> None:
    batches = await _collect(_Page(icon=False), (COMMUNITY,))

    assert len(batches) == 2
    assert len(batches[0].posts) == 1
    assert batches[0].communities == ()
    assert batches[1].posts == []
    assert batches[1].communities[0].community == COMMUNITY
    assert batches[1].communities[0].icon_url == ""


async def test_header_waits_for_icon_that_hydrates_after_scrolling() -> None:
    page = _Page(icon=False, icon_after_scroll=1)
    batches = await _collect(page, (COMMUNITY,))
    discoveries = [item for batch in batches for item in batch.communities]

    assert len(discoveries) == 1
    assert discoveries[0].icon_url == ICON_URL
    assert batches[0].communities == ()
    assert [post.post_id for batch in batches for post in batch.posts] == [
        "t3_abc",
    ]
