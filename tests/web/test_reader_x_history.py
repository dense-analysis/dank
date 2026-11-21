"""Select an older X conversation without replacing the displayed capture."""

import datetime as dt
from pathlib import Path
from typing import Literal, cast

import pytest
from test_reader_x_conversation import archive, selected, tweet

from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_x import post_x_content
from dank.web.reader_x_conversation import conversation_capture
from dank.web.reader_x_history import preserve_x_archives
from tests.api.clickhouse import QueryResults, asset, query_client, raw_post


@pytest.mark.parametrize("current_status", ["failed", "disabled", None])
async def test_history_recovers_only_the_archive(
    current_status: str | None,
) -> None:
    shown = selected()
    captured_at = shown.updated_at - dt.timedelta(days=1)
    key = shown.domain, shown.post_id
    earlier = tweet(shown.post_id)
    earlier["conversation_capture"] = archive([tweet("101", shown.post_id)])
    latest = tweet(shown.post_id, text="New text")

    if current_status is not None:
        latest["conversation_capture"] = {
            "status": current_status,
            "captured_at": shown.updated_at.isoformat(),
        }

    snapshots = await preserve_x_archives(query_client([raw_post(
        shown.domain, shown.post_id, earlier, scraped_at=captured_at,
    )]), [shown], {key: latest})
    merged = snapshots[key]
    result = conversation_capture(shown, merged, {}, include_posts=True)

    assert merged["legacy"] == latest["legacy"]
    assert result["status"] == "captured"
    assert [reply["id"] for reply in result["replies"]] == ["101"]
    assert result["latest_attempt_status"] == (
        current_status or "not_collected"
    )
    assert result["latest_attempt_at"] == shown.updated_at.isoformat()


@pytest.mark.parametrize("reason", ["newer", "foreign", "broken", "invalid"])
async def test_history_rejects_wrong_versions(
    reason: Literal["newer", "foreign", "broken", "invalid"],
) -> None:
    shown = selected()
    key = shown.domain, shown.post_id
    payload = tweet(shown.post_id)
    payload["conversation_capture"] = archive([tweet("101", shown.post_id)])
    captured_at = shown.updated_at - dt.timedelta(days=1)
    row = raw_post(*key, payload, scraped_at=captured_at)

    match reason:
        case "newer":
            row["scraped_at"] = shown.updated_at
        case "foreign":
            row["domain"] = "other.test"
        case "broken":
            row["payload"] = "{"
        case "invalid":
            row["payload"] = '{"conversation_capture":{"status":"failed"}}'

    expected = {key: tweet(shown.post_id, text="New text")}

    actual = await preserve_x_archives(query_client([row]), [shown], expected)

    assert actual == expected


async def test_history_leaves_a_saved_partial_archive_intact() -> None:
    shown = selected()
    current = tweet(shown.post_id)
    current["conversation_capture"] = archive([], status="partial")
    snapshots = {(shown.domain, shown.post_id): current}

    actual = await preserve_x_archives(query_client(), [shown], snapshots)

    assert actual == snapshots


async def test_detail_links_saved_parent_and_reuses_related_media(
    memory_path: Path,
) -> None:
    shown = selected("101")
    target = tweet("101", "100")
    child = tweet("102", "101")
    child["legacy"]["extended_entities"] = {"media": [{
        "type": "photo", "media_url_https": "https://pbs.twimg.com/photo.jpg",
    }]}
    target["conversation_capture"] = archive([child], target="101")
    parent = tweet("100")
    image = memory_path / "photo.jpg"
    image.touch()
    parent_url = "https://x.com/author/status/100"
    queries = QueryResults((
        [raw_post("x.com", "101", target)],
        [{"domain": "x.com", "post_id": "100", "url": parent_url}],
        [raw_post("x.com", "100", parent), raw_post("x.com", "999", "{"),
         raw_post("x.com", "998", child)],
        [asset("x.com", "102", "https://pbs.twimg.com/photo.jpg", image),
         asset("x.com", "outside", "https://pbs.twimg.com/photo.jpg", image)],
    ))
    content = await post_x_content(
        cast(ClickHouseClient, queries), [shown], {}, memory_path,
        include_discussion=True,
    )
    result = content[(shown.domain, shown.post_id)]

    assert result["reply_to"]["collected"]
    assert result["reply_to"]["url"] == parent_url
    assert [
        item["id"] for item in result["conversation"]["context"]
    ] == ["100"]
    [reply] = result["conversation"]["replies"]
    assert reply["id"] == "102"
    assert reply["x"]["media"][0]["url"] == "/assets/photo.jpg"
    assert not queries.responses
