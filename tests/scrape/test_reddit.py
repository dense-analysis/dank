from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import patch

import pytest

from dank.config import (
    BrowserScrapeSettings,
    RedditSettings,
    RedditSort,
    RedditTimeFilter,
    SourceConfig,
)
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.reddit import (
    RedditScrapeError,
    community_url,
    extract_posts_and_assets,
    scrape_reddit_communities,
)
from dank.scrape.reddit.html import extract_html_posts
from dank.scrape.reddit.payloads import extract_json_posts
from dank.scrape.zendriver import NetworkResponse


def _card(
    post_id: str = "abc",
    *,
    community: str = "LocalLLaMA",
    extra: str = "",
) -> str:
    return (
        f'<shreddit-post id="t3_{post_id}" post-title="A &amp; B" '
        f'permalink="/r/{community}/comments/{post_id}/a_b/" '
        'author="example" created-timestamp="2026-10-15T07:30:12.231000+0000" '
        f'post-type="text" score="123" comment-count="7" {extra}>'
        '<a slot="title">A &amp; B</a>'
        '<div slot="text-body"><p>Public <strong>body</strong>.</p></div>'
        "</shreddit-post>"
    )


def _json_post(
    post_id: str = "abc",
    /,
    **changes: object,
) -> dict[str, object]:
    return {
        "id": post_id,
        "name": f"t3_{post_id}",
        "title": "A post",
        "subreddit": "LocalLLaMA",
        "author": "example",
        "created_utc": 1760000000,
        "permalink": f"/r/LocalLLaMA/comments/{post_id}/a_post/",
        "url": f"https://i.redd.it/{post_id}.png",
        "score": 12,
        "num_comments": 3,
        "is_self": False,
        **changes,
    }


def _response(body: str, *, status: int = 200) -> NetworkResponse:
    return NetworkResponse(
        "https://www.reddit.com/svc/shreddit/community-more-posts/best/",
        status,
        "text/vnd.reddit.partial+html",
        body,
        "request-1",
        "Fetch",
    )


def test_html_extracts_live_listing_attributes_and_original_evidence() -> None:
    source = _card()
    posts = extract_html_posts(source, "localllama")

    assert len(posts) == 1
    assert posts[0]["id"] == "t3_abc"
    assert posts[0]["title"] == "A & B"
    assert posts[0]["author"] == "example"
    assert posts[0]["selftext_html"] == "<p>Public <strong>body</strong>.</p>"
    assert posts[0]["score"] == 123
    assert posts[0]["num_comments"] == 7
    assert posts[0]["created_utc"] == 1792049412.231
    assert posts[0]["raw"] == {"html": source}


def test_html_filters_promoted_and_other_communities() -> None:
    source = _card("one", extra='promoted=""') + _card(
        "two",
        community="Python",
    )
    source += _card("three", extra='is-ad="true"') + _card("four")

    assert [
        post["id"] for post in extract_html_posts(source, "LocalLLaMA")
    ] == ["t3_four"]


def test_html_preserves_body_across_void_tags_and_lines() -> None:
    source = _card().replace(
        "<p>Public <strong>body</strong>.</p>",
        '<p>Hello<br>world &amp; more</p>\n<img src="https://i.redd.it/test.png">',
    )
    post = extract_html_posts(source, "LocalLLaMA")[0]

    assert "Hello<br>world &amp; more" in str(post["selftext_html"])
    assert post["media"] == [
        {"url": "https://i.redd.it/test.png", "asset_type": "photo"},
    ]
    assert post["raw"] == {"html": source}


def test_json_listing_retains_original_and_skips_nested_crossposts() -> None:
    original = _json_post(crosspost_parent_list=[_json_post("older")])
    payload = {"data": {"children": [{"kind": "t3", "data": original}]}}
    posts = extract_json_posts(payload, "LocalLLaMA")

    assert len(posts) == 1
    assert posts[0]["raw"] == original
    assert posts[0]["id"] == "t3_abc"
    assert posts[0]["media"] == [
        {"url": "https://i.redd.it/abc.png", "asset_type": "photo"},
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"promoted": True},
        {"permalink": "https://evil.example/r/LocalLLaMA/comments/abc/post/"},
        {"permalink": "/r/Python/comments/abc/post/"},
        {"name": "t3_mismatch"},
        {"title": ""},
    ],
)
def test_json_rejects_invalid_or_unwanted_posts(
    changes: dict[str, object],
) -> None:
    assert extract_json_posts(_json_post(**changes), "LocalLLaMA") == []


def test_json_skips_ad_wrappers_and_handles_graphql_nesting() -> None:
    payload = {
        "data": {
            "feed": {
                "edges": [
                    {"node": _json_post("one")},
                    {"isPromoted": True, "node": _json_post("two")},
                ],
            },
        },
    }

    assert [
        post["id"] for post in extract_json_posts(payload, "LocalLLaMA")
    ] == ["t3_one"]


def test_network_and_dom_deduplicate_and_enforce_exact_cap() -> None:
    seen_posts: set[str] = set()
    seen_assets: set[str] = set()
    response = _response(json.dumps([_json_post("abc"), _json_post("def")]))
    batch = extract_posts_and_assets(
        [response],
        "LocalLLaMA",
        seen_posts,
        seen_assets,
        max_posts=1,
        dom_html=_card("abc") + _card("ghi"),
    )

    assert [post.post_id for post in batch.posts] == ["t3_abc"]
    assert seen_posts == {"t3_abc"}
    assert len(batch.assets) == 1
    assert batch.posts[0].source == "reddit"
    assert batch.posts[0].domain == "reddit.com"
    assert batch.posts[0].request_url == response.url
    assert (
        batch.posts[0].url
        == "https://www.reddit.com/r/LocalLLaMA/comments/abc/a_post/"
    )
    assert (
        extract_posts_and_assets(
            [response],
            "LocalLLaMA",
            seen_posts,
            seen_assets,
            max_posts=1,
        ).posts
        == []
    )


def test_html_network_response_and_dom_fallback() -> None:
    batch = extract_posts_and_assets(
        [
            _response("broken json"),
            _response(_card("aaa")),
            _response(_card("bad"), status=403),
        ],
        "LocalLLaMA",
        set(),
        set(),
        max_posts=10,
        dom_html=_card("aaa") + _card("bbb"),
        request_url="https://www.reddit.com/r/LocalLLaMA/best/",
    )

    assert [post.post_id for post in batch.posts] == ["t3_aaa", "t3_bbb"]
    assert batch.posts[1].request_url.endswith("/best/")
    assert "html" in json.loads(batch.posts[0].payload)["raw"]


def test_top_day_default_and_explicit_listing_options() -> None:
    assert community_url("LocalLLaMA").endswith("/LocalLLaMA/top/?t=day")
    assert community_url("LocalLLaMA", sort=RedditSort.BEST).endswith("/best/")
    assert community_url(
        "Python", sort=RedditSort.TOP, time_filter=RedditTimeFilter.WEEK,
    ).endswith(
        "?t=week",
    )


@pytest.mark.parametrize(
    "community,sort,time_filter",
    [
        ("../login", "best", "day"),
        ("Python", "bad", "day"),
        ("Python", "top", "invalid"),
    ],
)
def test_invalid_listing_options(
    community: str,
    sort: str,
    time_filter: str,
) -> None:
    with pytest.raises(ValueError):
        community_url(
            community, sort=cast(RedditSort, sort),
            time_filter=cast(RedditTimeFilter, time_filter),
        )


class _Page:
    def __init__(self, snapshots: list[str], *, body: str = "") -> None:
        self.snapshots = snapshots
        self.body = body
        self.scrolls = 0
        self.closed = False
        self.urls: list[str] = []

    async def activate(self) -> None:
        pass

    async def send(
        self,
        command: Generator[dict[str, object], object, object],
    ) -> None:
        params = next(command)
        self.urls.append(str(cast(dict[str, object], params["params"])["url"]))

    async def get(self, url: str) -> _Page:
        self.urls.append(url)
        return self

    async def wait_for_ready_state(self, **_kwargs: object) -> None:
        pass

    async def sleep(self, _seconds: float) -> None:
        pass

    async def evaluate(self, script: str) -> str:
        if script == "1":
            return "1"

        if script.startswith("JSON.stringify"):
            return json.dumps(
                {
                    "url": self.urls[-1],
                    "text": self.body,
                    "html": self.snapshots[
                        min(self.scrolls, len(self.snapshots) - 1)
                    ],
                },
            )

        self.scrolls += 1
        return ""

    async def close(self) -> None:
        self.closed = True


class _Browser:
    def __init__(self, page: _Page) -> None:
        self.page = page
        self.new_tab = False

    async def get(self, _url: str, *, new_tab: bool) -> _Page:
        self.new_tab = new_tab
        return self.page


class _Session:
    def __init__(self, page: _Page) -> None:
        self.browser = _Browser(page)

    async def get_browser(self) -> _Browser:
        return self.browser


class _Capture:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def drain(self, **_kwargs: object) -> list[NetworkResponse]:
        return []


async def _collect(
    page: _Page,
    settings: RedditSettings,
) -> tuple[list[str], _Capture]:
    capture = _Capture()
    session = _Session(page)

    with (
        patch("dank.scrape.reddit.NetworkCapture", return_value=capture),
        patch("dank.scrape.reddit.asyncio.sleep"),
    ):
        batches = [
            batch
            async for batch in scrape_reddit_communities(
                settings,
                ("LocalLLaMA",),
                cast(Any, session),
            )
        ]

    assert session.browser.new_tab
    assert page.closed
    assert capture.started and capture.stopped

    return [post.post_id for batch in batches for post in batch.posts], capture


async def test_collection_scrolls_for_new_posts_and_stops_at_exact_limit() -> (
    None
):
    page = _Page([_card("aaa"), _card("aaa") + _card("bbb") + _card("ccc")])
    posts, _ = await _collect(
        page, RedditSettings(scrape=BrowserScrapeSettings(max_posts=2)),
    )

    assert posts == ["t3_aaa", "t3_bbb"]
    assert page.scrolls == 1


async def test_collection_stops_on_stall() -> None:
    page = _Page([_card()])
    posts, _ = await _collect(page, RedditSettings(stall_scrolls=2))

    assert posts == ["t3_abc"]
    assert page.scrolls == 2


async def test_collection_stops_at_scroll_limit() -> None:
    page = _Page([_card("aaa"), _card("bbb"), _card("ccc"), _card("ddd")])
    posts, _ = await _collect(
        page, RedditSettings(scrape=BrowserScrapeSettings(max_scrolls=2)),
    )

    assert posts == ["t3_aaa", "t3_bbb", "t3_ccc"]
    assert page.scrolls == 2


async def test_collection_handles_initial_delayed_content() -> None:
    page = _Page(["", _card("aaa")])
    posts, _ = await _collect(
        page, RedditSettings(scrape=BrowserScrapeSettings(max_posts=1)),
    )

    assert posts == ["t3_aaa"]
    assert page.scrolls == 1


async def test_pagination_resumes_after_details_hide_the_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FocusedPage(_Page):
        active = False

        async def activate(self) -> None:
            self.active = True

        async def send(self, command: Any) -> None:
            assert self.active, "Navigation must render in the active tab"
            await super().send(command)

        async def evaluate(self, script: str) -> str:
            if not script.startswith("JSON.stringify") and not self.active:
                # Background scrolling never triggers the next-page loader.
                return ""

            return await super().evaluate(script)

    page = _FocusedPage([
        "".join(_card(str(i)) for i in range(3)),
        "".join(_card(str(i)) for i in range(12)),
    ])

    async def visit_details(batch: Any, *args: Any) -> Any:
        page.active = False
        return batch

    monkeypatch.setattr(
        "dank.scrape.reddit._capture_batch_details", visit_details,
    )
    posts, _ = await _collect(
        page, RedditSettings(scrape=BrowserScrapeSettings(max_posts=8)),
    )
    assert posts == [f"t3_{i}" for i in range(8)]
    assert page.scrolls == 1


@pytest.mark.parametrize(
    "body,expected",
    [
        (
            "You've been blocked by network security.",
            "access requires attention",
        ),
        ("This community is private.", "access requires attention"),
        ("", "No public posts found"),
    ],
)
async def test_collection_reports_blocked_or_empty_and_cleans_up(
    body: str,
    expected: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    page = _Page([""], body=body)
    capture = _Capture()

    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    token = current_source.set(stats)

    try:
        with (
            patch("dank.scrape.reddit.NetworkCapture", return_value=capture),
            patch("dank.scrape.reddit.asyncio.sleep"),
        ):
            batches = [
                batch
                async for batch in scrape_reddit_communities(
                    RedditSettings(stall_scrolls=2),
                    ("LocalLLaMA",),
                    cast(Any, _Session(page)),
                )
            ]
    finally:
        current_source.reset(token)

    assert batches == []
    assert expected in caplog.text
    assert stats.counts["fetch_failures"] == 1
    assert stats.error_type == RedditScrapeError.__name__
    assert capture.stopped
    assert page.closed


async def test_explicit_empty_listing_is_not_a_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO", logger="dank.scrape.reddit")
    page = _Page([""], body="This community doesn't have any posts yet")
    stats = SourceMetrics(1, SourceConfig("reddit.com", ()))
    token = current_source.set(stats)

    try:
        posts, _ = await _collect(page, RedditSettings())
    finally:
        current_source.reset(token)

    assert posts == []
    assert page.scrolls == 0
    assert stats.counts["fetch_failures"] == 0
    assert "reason=empty listing" in caplog.text


async def test_cancellation_is_not_treated_as_a_community_failure() -> None:
    page = _Page([""])
    capture = _Capture()

    with (
        patch("dank.scrape.reddit.NetworkCapture", return_value=capture),
        patch.object(page, "send", side_effect=asyncio.CancelledError()),
        pytest.raises(asyncio.CancelledError),
    ):
        _ = [batch async for batch in scrape_reddit_communities(
            RedditSettings(), ("first", "second"), cast(Any, _Session(page)),
        )]

    assert capture.stopped
    assert page.closed


def test_sparse_network_post_is_enriched_with_listing_body() -> None:
    batch = extract_posts_and_assets(
        [_response(json.dumps(_json_post()))],
        "LocalLLaMA",
        set(),
        set(),
        max_posts=10,
        dom_html=_card(),
    )
    payload = json.loads(batch.posts[0].payload)

    assert "Public <strong>body" in payload["selftext_html"]
    assert "captures" in payload["raw"]


def test_repeat_post_can_discover_newly_loaded_media() -> None:
    seen_posts: set[str] = set()
    seen_assets: set[str] = set()
    extract_posts_and_assets(
        [],
        "LocalLLaMA",
        seen_posts,
        seen_assets,
        max_posts=10,
        dom_html=_card(),
    )
    batch = extract_posts_and_assets(
        [],
        "LocalLLaMA",
        seen_posts,
        seen_assets,
        max_posts=10,
        dom_html=_card().replace(
            "</shreddit-post>",
            '<img src="https://i.redd.it/later.png"></shreddit-post>',
        ),
    )

    assert batch.posts == []
    assert [asset.url for asset in batch.assets] == [
        "https://i.redd.it/later.png",
    ]


def test_shared_asset_has_each_post_as_owner() -> None:
    batch = extract_posts_and_assets(
        [
            _response(
                json.dumps(
                    [
                        _json_post("aaa", url="https://i.redd.it/shared.png"),
                        _json_post("bbb", url="https://i.redd.it/shared.png"),
                    ],
                ),
            ),
        ],
        "LocalLLaMA",
        set(),
        set(),
        max_posts=10,
    )

    assert {asset.post_id for asset in batch.assets} == {"t3_aaa", "t3_bbb"}


def test_nested_crosspost_does_not_replace_outer_body() -> None:
    source = _card("outer").replace(
        "</shreddit-post>",
        _card("inner") + "</shreddit-post>",
    )
    posts = extract_html_posts(source, "LocalLLaMA")

    assert [post["id"] for post in posts] == ["t3_outer"]
    assert posts[0]["selftext_html"] == "<p>Public <strong>body</strong>.</p>"
    assert posts[0]["raw"] == {"html": source}


def test_promoted_wrappers_are_excluded_in_network_html() -> None:
    source = '<div data-promoted="true">' + _card("ad") + "</div>" + _card()

    assert [
        post["id"] for post in extract_html_posts(source, "LocalLLaMA")
    ] == ["t3_abc"]


def test_malformed_links_do_not_discard_valid_posts() -> None:
    payload = [
        _json_post("bad", permalink="https://["),
        _json_post("good", url="https://["),
    ]
    posts = extract_json_posts(payload, "LocalLLaMA")

    assert [post["id"] for post in posts] == ["t3_good"]
    assert posts[0]["media"] == []


def test_title_entities_and_flair_are_decoded_once() -> None:
    source = _card().replace(
        'post-title="A &amp; B"',
        'post-title="&amp;lt;script&amp;gt;"',
    )
    source = source.replace(
        "</shreddit-post>",
        "<shreddit-post-flair><span>News &amp; Updates</span>"
        "</shreddit-post-flair></shreddit-post>",
    )
    post = extract_html_posts(source, "LocalLLaMA")[0]

    assert post["title"] == "&lt;script&gt;"
    assert post["link_flair_text"] == "News & Updates"


def test_direct_image_does_not_also_collect_its_preview() -> None:
    post = extract_json_posts(
        _json_post(
            preview={
                "images": [
                    {
                        "source": {
                            "url": "https://preview.redd.it/abc.png?width=640",
                        },
                    },
                ],
            },
        ),
        "LocalLLaMA",
    )[0]

    assert post["media"] == [
        {"url": "https://i.redd.it/abc.png", "asset_type": "photo"},
    ]


async def test_restriction_phrase_in_normal_post_is_not_access_failure() -> (
    None
):
    page = _Page([_card()], body="Discussion: verify you are human")
    posts, _ = await _collect(
        page, RedditSettings(scrape=BrowserScrapeSettings(max_posts=1)),
    )

    assert posts == ["t3_abc"]


def test_ad_wrapper_with_self_closing_tags_stays_excluded() -> None:
    source = (
        '<div data-promoted="true"><br/><img src="x"/>'
        + _card("ad")
        + "</div>"
        + _card()
    )

    assert [
        post["id"] for post in extract_html_posts(source, "LocalLLaMA")
    ] == ["t3_abc"]


def test_dom_enriches_missing_metrics_and_destination() -> None:
    sparse = _json_post(url="", score=None, num_comments=None)
    dom = _card(extra='content-href="https://example.com/article"')
    batch = extract_posts_and_assets(
        [_response(json.dumps(sparse))],
        "LocalLLaMA",
        set(),
        set(),
        max_posts=10,
        dom_html=dom,
    )
    post = json.loads(batch.posts[0].payload)

    assert post["score"] == 123
    assert post["num_comments"] == 7
    assert post["url"] == "https://example.com/article"


def test_gallery_preserves_distinct_images() -> None:
    post = extract_json_posts(
        _json_post(
            url="https://www.reddit.com/gallery/abc",
            media_metadata={
                "one": {"s": {"u": "https://i.redd.it/one.png"}},
                "two": {"s": {"u": "https://i.redd.it/two.png"}},
            },
        ),
        "LocalLLaMA",
    )[0]

    assert post["media"] == [
        {"url": "https://i.redd.it/one.png", "asset_type": "photo"},
        {"url": "https://i.redd.it/two.png", "asset_type": "photo"},
    ]


def test_semantic_body_excludes_controls_but_keeps_post_words() -> None:
    body = "<p>Read more about this launch.</p><p>Second paragraph.</p>"
    wrapper = (
        '<div class="overflow-wrapper">'
        f'<div property="schema:articleBody">{body}</div>'
        '<div class="overflow-cover"></div>'
        '<button id="t3_abc-read-more-button">Read more</button>'
        '</div>'
    )
    source = _card().replace(
        "<p>Public <strong>body</strong>.</p>", wrapper,
    )
    post = extract_html_posts(source, "LocalLLaMA")[0]

    assert post["selftext_html"] == body
    assert post["raw"] == {"html": source}
