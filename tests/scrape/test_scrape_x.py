import json
import pathlib
from typing import Any, cast

import pytest

from dank.scrape.x import extract_posts_and_assets
from dank.scrape.x.navigation import (
    has_login_prompt as _has_login_prompt,
    is_login_page as _is_login_page,
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
    seen_references: set[str] = set()

    posts, assets = extract_posts_and_assets(
        [response],
        seen_posts,
        seen_assets,
        seen_references,
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
        seen_references,
    )

    assert not posts_repeat
    assert not assets_repeat


class _FakeLoginPromptPage:
    def __init__(
        self,
        *,
        location: object,
        prompt_present: bool,
    ) -> None:
        self.location = location
        self.prompt_present = prompt_present
        self.last_selector: str | None = None

    async def evaluate(self, _script: str) -> object:
        return self.location

    async def query_selector(self, selector: str) -> object | None:
        self.last_selector = selector

        return object() if self.prompt_present else None


async def test_has_login_prompt_queries_without_a_selector_wait() -> None:
    page = _FakeLoginPromptPage(location=None, prompt_present=True)

    assert await _has_login_prompt(cast(Any, page)) is True
    assert page.last_selector == 'a[href="/i/flow/signup"]'


async def test_has_login_prompt_handles_absent_prompt() -> None:
    page = _FakeLoginPromptPage(location=None, prompt_present=False)

    assert await _has_login_prompt(cast(Any, page)) is False


async def test_is_login_page_checks_prompt_on_account_page() -> None:
    page = _FakeLoginPromptPage(
        location="https://x.com/example",
        prompt_present=False,
    )

    assert await _is_login_page(cast(Any, page)) is False
    assert page.last_selector == 'a[href="/i/flow/signup"]'


async def test_is_login_page_checks_selector_for_missing_location() -> None:
    page = _FakeLoginPromptPage(location=None, prompt_present=True)

    assert await _is_login_page(cast(Any, page)) is True


@pytest.mark.parametrize("path", [
    "/i/flow/login", "/i/jf/onboarding/web?mode=login", "/account/access",
])
async def test_login_and_verification_redirects_require_sign_in(
    path: str,
) -> None:
    page = _FakeLoginPromptPage(
        location=f"https://x.com{path}", prompt_present=False,
    )
    assert await _is_login_page(cast(Any, page)) is True
