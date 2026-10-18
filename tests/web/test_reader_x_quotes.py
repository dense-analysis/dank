from __future__ import annotations

import copy
import pathlib
from typing import Any

import pytest
from test_reader import FakeClickHouse
from test_reader_x_conversation import archive, raw_row, selected, tweet

from dank.web.reader_x import post_x_content
from dank.web.reader_x_quotes import quote_target, quoted_post

IMAGE = "https://pbs.twimg.com/media/quote.jpg"
VIDEO = "https://video.twimg.com/quote.mp4"


def with_quote(parent: dict[str, Any], child: dict[str, Any]):
    parent["legacy"]["quoted_status_id_str"] = child["rest_id"]
    parent["quoted_status_result"] = {"result": {
        "__typename": "TweetWithVisibilityResults", "tweet": child,
    }}

    return parent


def quote_tweet() -> dict[str, Any]:
    post = tweet("200", root="200", handle="quoted", text="Quoted snapshot")
    post["legacy"]["extended_entities"] = {"media": [{
        "type": "video", "media_url_https": IMAGE,
        "expanded_url": "https://x.com/quoted/status/200/video/1",
        "video_info": {"aspect_ratio": [9, 16], "variants": [{
            "url": VIDEO, "content_type": "video/mp4", "bitrate": 500,
        }]},
    }]}
    post["note_tweet"] = {"note_tweet_results": {"result": {
        "text": 'Full quoted note\n\n<script>Literal text</script>',
    }}}

    return post


@pytest.mark.parametrize("discussion", [False, True])
async def test_quotes_preserve_embedded_version_and_saved_media(
    tmp_path: pathlib.Path, *, discussion: bool,
):
    child = quote_tweet()
    parent = with_quote(tweet("100"), child)
    parent["conversation_capture"] = archive([])
    newer = copy.deepcopy(child)
    newer["note_tweet"]["note_tweet_results"]["result"]["text"] = "Newer edit"
    video = tmp_path / "quote.mp4"
    video.touch()
    fake = FakeClickHouse([raw_row(parent)], [raw_row(newer)], [{
        "domain": "x.com", "post_id": "200", "url": VIDEO,
        "local_path": str(video), "content_type": "video/mp4",
        "size_bytes": 0,
    }])
    result = await post_x_content(
        fake, [selected()], {}, tmp_path,  # type: ignore[arg-type]
        include_discussion=discussion,
    )
    quote = result["x.com", "100"]["quote"]
    assert quote["id"] == "200"
    assert quote["collected"] is True
    assert quote["post"]["x"]["name"] == "Quoted"
    assert "Full quoted note" in quote["post"]["html"]
    assert "Newer edit" not in quote["post"]["html"]
    assert "<script>" not in quote["post"]["html"]
    assert "&lt;script&gt;" in quote["post"]["html"]
    assert quote["post"]["x"]["media"][0]["url"] == "/assets/quote.mp4"
    assert quote["post"]["x"]["media"][0]["remote"] is False
    assert quote["post"]["x"]["media"][0]["aspect_ratio"] == 9 / 16
    assert any(params.get("x_quote_keys") == [("x.com", "200")]
               for _, params in fake.calls)
    assert not fake.results


async def test_quotes_inside_saved_conversation_keep_their_own_context(
    tmp_path: pathlib.Path,
):
    parent = tweet("100", replies=1)
    reply = with_quote(tweet("101", "100"), quote_tweet())
    parent["conversation_capture"] = archive([reply])
    fake = FakeClickHouse([raw_row(parent)], [], [])
    result = await post_x_content(
        fake, [selected()], {}, tmp_path,  # type: ignore[arg-type]
        include_discussion=True,
    )
    replies = result["x.com", "100"]["conversation"]["replies"]
    assert [node["id"] for node in replies] == ["101"]
    assert replies[0]["x"]["quote"]["post"]["id"] == "200"
    assert replies[0]["x"]["quote"]["collected"] is False


def test_missing_quote_falls_back_to_saved_post_but_never_another_domain():
    parent = with_quote(tweet("100"), {
        "__typename": "Tweet", "rest_id": "200",
    })
    child = quote_tweet()
    recovered = quoted_post(parent, "x.com", {}, {("x.com", "200"): child})
    assert recovered["quote"]["post"]["id"] == "200"
    missing = quoted_post(parent, "x.com", {}, {("twitter.com", "200"): child})
    assert missing == {"quote": {
        "id": "200", "domain": "x.com",
        "url": "https://x.com/i/status/200", "collected": False,
    }}


def test_quote_identity_and_nesting_are_bounded():
    child = with_quote(quote_tweet(), tweet("300"))
    parent = with_quote(tweet("100"), child)
    result = quoted_post(parent, "x.com", {}, {})
    assert "quote" not in result["quote"]["post"]["x"]
    parent["legacy"]["quoted_status_id_str"] = "999"
    assert "post" not in quoted_post(parent, "x.com", {}, {})["quote"]
    parent["legacy"]["quoted_status_id_str"] = "100"
    assert quote_target(parent) is None
    assert quote_target({"legacy": {"quoted_status_id_str": "bad"}}) is None
    parent = with_quote(tweet("100"), {
        "rest_id": "200", "__typename": "TweetTombstone",
    })
    assert "post" not in quoted_post(parent, "x.com", {}, {})["quote"]
