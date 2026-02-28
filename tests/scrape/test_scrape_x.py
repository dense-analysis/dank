import json
import pathlib
from typing import Any, cast

from dank.scrape.x import (
    LOGIN_PROMPT_TIMEOUT_SECONDS,
    _has_login_prompt,  # pyright: ignore[reportPrivateUsage]
    _is_login_location,  # pyright: ignore[reportPrivateUsage]
    _is_login_page,  # pyright: ignore[reportPrivateUsage]
    _is_x_location,  # pyright: ignore[reportPrivateUsage]
    _normalize_x_path,  # pyright: ignore[reportPrivateUsage]
    extract_posts_and_assets,
)
from dank.scrape.zendriver import NetworkResponse


def _load_fixture(name: str) -> dict[str, object]:
    path = pathlib.Path(__file__).parent.parent / "fixtures" / name

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def test_extract_posts_and_assets_from_network_payload() -> None:
    payload = _load_fixture("UserTweetsWactherGuruExample.json")
    response = NetworkResponse(
        url="https://x.com/i/api/graphql/abc123/UserTweets",
        status=200,
        mime_type="application/json",
        body=json.dumps(payload),
        request_id="request-1",
        resource_type="XHR",
    )
    seen_posts: set[str] = set()
    seen_assets: set[str] = set()

    posts, assets = extract_posts_and_assets(
        [response],
        seen_posts,
        seen_assets,
    )

    assert len(posts) >= 10
    assert len(assets) >= 10
    assert any(asset.asset_type == "photo" for asset in assets)
    assert any(
        asset.url.startswith("https://pbs.twimg.com/media/")
        for asset in assets
    )

    posts_repeat, assets_repeat = extract_posts_and_assets(
        [response],
        seen_posts,
        seen_assets,
    )

    assert not posts_repeat
    assert not assets_repeat


def test_is_x_location_accepts_only_x_domains() -> None:
    assert _is_x_location("https://x.com/dense") is True
    assert _is_x_location("https://www.x.com/dense") is True
    assert _is_x_location("https://example.com/dense") is False


def test_is_login_location_checks_x_login_paths() -> None:
    assert _is_login_location("https://x.com/i/flow/login") is True
    assert _is_login_location("https://x.com/login") is True
    assert _is_login_location("https://x.com/dense") is False


def test_normalize_x_path_normalizes_case_and_trailing_slashes() -> None:
    assert _normalize_x_path("Dense/") == "/dense"
    assert _normalize_x_path("/DENSE/") == "/dense"
    assert _normalize_x_path("/") == "/"


class _FakeLoginPromptPage:
    def __init__(
        self,
        *,
        location: object,
        select_raises: Exception | None,
    ) -> None:
        self.location = location
        self.select_raises = select_raises
        self.last_timeout: float | None = None

    async def evaluate(self, _script: str) -> object:
        return self.location

    async def select(self, *_args: object, **kwargs: object) -> object:
        timeout = kwargs.get("timeout")

        if isinstance(timeout, int | float):
            self.last_timeout = float(timeout)

        if self.select_raises is not None:
            raise self.select_raises

        return object()


async def test_has_login_prompt_uses_short_selector_timeout() -> None:
    page = _FakeLoginPromptPage(location=None, select_raises=None)

    assert await _has_login_prompt(cast(Any, page)) is True
    assert page.last_timeout == LOGIN_PROMPT_TIMEOUT_SECONDS


async def test_has_login_prompt_handles_timeout() -> None:
    page = _FakeLoginPromptPage(location=None, select_raises=TimeoutError())

    assert await _has_login_prompt(cast(Any, page)) is False


async def test_is_login_page_skips_selector_for_non_login_url() -> None:
    page = _FakeLoginPromptPage(
        location="https://x.com/example",
        select_raises=None,
    )

    assert await _is_login_page(cast(Any, page)) is False
    assert page.last_timeout is None


async def test_is_login_page_checks_selector_for_missing_location() -> None:
    page = _FakeLoginPromptPage(location=None, select_raises=None)

    assert await _is_login_page(cast(Any, page)) is True
