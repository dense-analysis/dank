"""/assets/ and /static/: seeking, revalidation and what stays hidden."""

import email.utils
import os
import pathlib

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from starlette.types import Message

from dank.api.app import create_app
from dank.api.files.static import FileRoots
from dank.config import Settings
from tests.api.client import http_client

# More than one of Starlette's 64 KiB chunks, as any real video is.
VIDEO = bytes(range(256)) * 4096
SIZE = len(VIDEO)


@pytest.fixture
def video(assets: pathlib.Path) -> pathlib.Path:
    path = assets / "example.com" / "clip.mp4"
    path.parent.mkdir()
    path.write_bytes(VIDEO)

    return path


URL = "/assets/example.com/clip.mp4"


@pytest.mark.usefixtures("video")
@pytest.mark.parametrize(("header", "start", "end"), [
    ("bytes=100-199", 100, 199),
    ("bytes=0-0", 0, 0),
    # What a browser asks for when seeking: from here to the end.
    ("bytes=500000-", 500000, SIZE - 1),
    ("bytes=-10", SIZE - 10, SIZE - 1),
    # Past the end is cut short, not refused.
    (f"bytes={SIZE - 5}-{SIZE + 100}", SIZE - 5, SIZE - 1),
])
async def test_ranges_answer_with_only_the_bytes_asked_for(
    client: AsyncClient, header: str, start: int, end: int,
) -> None:
    response = await client.get(URL, headers={"range": header})

    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes {start}-{end}/{SIZE}"
    assert response.headers["content-length"] == str(end - start + 1)
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-type"] == "video/mp4"
    assert response.content == VIDEO[start:end + 1]


@pytest.mark.usefixtures("video")
async def test_a_whole_file_says_it_can_seek(client: AsyncClient) -> None:
    response = await client.get(URL)

    assert response.status_code == 200
    assert response.content == VIDEO
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-length"] == str(SIZE)
    # Downloaded media keeps its validators and has no cache policy of its own.
    assert "etag" in response.headers
    assert "last-modified" in response.headers
    assert "cache-control" not in response.headers


@pytest.mark.usefixtures("video")
async def test_a_range_past_the_end_is_unsatisfiable(
    client: AsyncClient,
) -> None:
    response = await client.get(URL, headers={"range": f"bytes={SIZE}-"})

    assert response.status_code == 416
    assert response.headers["content-range"] == f"bytes */{SIZE}"


@pytest.mark.usefixtures("video")
async def test_matching_etags_revalidate(client: AsyncClient) -> None:
    etag = (await client.get(URL)).headers["etag"]

    fresh = await client.get(URL, headers={"if-none-match": etag})
    weak = await client.get(URL, headers={"if-none-match": f"W/{etag}"})
    changed = await client.get(URL, headers={"if-none-match": '"other"'})

    assert (fresh.status_code, fresh.content) == (304, b"")
    assert fresh.headers["etag"] == etag
    assert weak.status_code == 304
    assert changed.status_code == 200


async def test_modified_dates_revalidate(
    client: AsyncClient, video: pathlib.Path,
) -> None:
    os.utime(video, (1_000_000, 1_000_000))
    modified = (await client.get(URL)).headers["last-modified"]
    earlier = email.utils.formatdate(999_999, usegmt=True)

    fresh = await client.get(URL, headers={"if-modified-since": modified})
    stale = await client.get(URL, headers={"if-modified-since": earlier})

    assert fresh.status_code == 304
    assert stale.status_code == 200


@pytest.mark.usefixtures("video")
async def test_if_range_only_honors_the_range_while_the_file_is_unchanged(
    client: AsyncClient,
) -> None:
    whole = await client.get(URL)
    ask = {"range": "bytes=0-9"}

    by_etag = await client.get(
        URL, headers={**ask, "if-range": whole.headers["etag"]},
    )
    by_date = await client.get(
        URL, headers={**ask, "if-range": whole.headers["last-modified"]},
    )
    changed = await client.get(URL, headers={**ask, "if-range": '"other"'})

    assert (by_etag.status_code, by_etag.content) == (206, VIDEO[:10])
    assert (by_date.status_code, by_date.content) == (206, VIDEO[:10])
    assert (changed.status_code, changed.content) == (200, VIDEO)


@pytest.mark.usefixtures("video")
async def test_head_describes_a_range_without_a_body(
    client: AsyncClient,
) -> None:
    whole = await client.head(URL)
    part = await client.head(URL, headers={"range": "bytes=0-9"})

    assert (whole.status_code, whole.content) == (200, b"")
    assert whole.headers["content-length"] == str(SIZE)
    assert whole.headers["accept-ranges"] == "bytes"
    assert (part.status_code, part.content) == (206, b"")
    assert part.headers["content-length"] == "10"


@pytest.mark.usefixtures("video")
async def test_head_leaves_the_file_unread(app: FastAPI) -> None:
    # A HEAD run as a GET would stream the whole file and throw it away,
    # which on a large video takes as long as downloading it.
    sent: list[Message] = []

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        sent.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "method": "HEAD",
            "scheme": "http",
            "path": URL,
            "raw_path": URL.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [],
            "server": ("dank.test", 80),
            "client": ("127.0.0.1", 1),
        },
        receive,
        send,
    )

    assert [message["type"] for message in sent] == [
        "http.response.start", "http.response.body",
    ]
    assert sent[0]["status"] == 200


@pytest.mark.parametrize(("name", "content_type"), [
    ("a.mp4", "video/mp4"),
    ("a.webm", "video/webm"),
    ("a.mp3", "audio/mpeg"),
    ("a.jpg", "image/jpeg"),
    ("a.png", "image/png"),
    ("a.gif", "image/gif"),
    ("a.webp", "image/webp"),
    # Plenty of downloads have no extension.
    ("no-extension", "application/octet-stream"),
])
async def test_content_types_follow_the_file_name(
    client: AsyncClient, assets: pathlib.Path, name: str, content_type: str,
) -> None:
    (assets / name).write_bytes(b"data")

    response = await client.get(f"/assets/{name}")

    assert response.headers["content-type"] == content_type


async def test_awkward_file_names_are_reachable(
    client: AsyncClient, assets: pathlib.Path,
) -> None:
    (assets / "a clip é#1.mp4").write_bytes(b"data")

    response = await client.get("/assets/a%20clip%20%C3%A9%231.mp4")

    assert (response.status_code, response.content) == (200, b"data")


@pytest.mark.parametrize("path", [
    "/assets/missing.mp4",
    # No directory index.
    "/assets/",
    "/assets/example.com",
    "/assets/example.com/",
    # Out of the data directory.
    "/assets/%2e%2e/secret.txt",
    "/assets/example.com/%2e%2e/%2e%2e/secret.txt",
    "/assets/link.txt",
    "/assets/nul%00.txt",
    pytest.param(f"/assets/{'x' * 5000}", id="too-long"),
])
async def test_only_files_inside_the_directory_are_served(
    client: AsyncClient,
    tmp_path: pathlib.Path,
    assets: pathlib.Path,
    video: pathlib.Path,
    path: str,
) -> None:
    (tmp_path / "secret.txt").write_text("secret")
    (assets / "link.txt").symlink_to(tmp_path / "secret.txt")

    response = await client.get(path)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("text/plain")
    assert b"secret" not in response.content


@pytest.mark.usefixtures("video")
async def test_files_are_read_only(client: AsyncClient) -> None:
    response = await client.post(URL, json={})

    assert response.status_code == 405
    assert response.headers["content-type"].startswith("text/plain")


async def test_static_files_are_never_cached(client: AsyncClient) -> None:
    get = await client.get("/static/app.css")
    head = await client.head("/static/app.css")
    part = await client.get("/static/app.css", headers={"range": "bytes=0-3"})
    fresh = await client.get(
        "/static/app.css", headers={"if-none-match": get.headers["etag"]},
    )

    assert get.status_code == 200
    assert get.headers["content-type"] == "text/css; charset=utf-8"

    for response in (get, head, part, fresh):
        assert response.headers["cache-control"] == (
            "no-store, no-cache, must-revalidate, max-age=0"
        )
        assert response.headers["pragma"] == "no-cache"
        assert response.headers["expires"] == "0"

    assert (head.status_code, head.content) == (200, b"")
    assert (part.status_code, part.content) == (206, b"body")
    assert fresh.status_code == 304


@pytest.mark.parametrize("path", ["/static/", "/static/missing.css"])
async def test_static_has_no_index_and_a_plain_404(
    client: AsyncClient, path: str,
) -> None:
    response = await client.get(path)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("text/plain")


async def test_static_is_only_served_when_it_exists(
    settings: Settings, roots: FileRoots, tmp_path: pathlib.Path,
) -> None:
    app = create_app(
        settings, page_size=50, roots=roots._replace(static=tmp_path / "no"),
    )

    async with http_client(app) as client:
        response = await client.get("/static/app.css")

    assert response.status_code == 404
