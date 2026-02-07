import json
import pathlib

from dank.scrape.x import extract_posts_and_assets
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
