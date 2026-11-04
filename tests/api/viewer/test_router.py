"""GET / and /post over HTTP, with a fake behind the store port."""

import datetime as dt
import html
import pathlib
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from dank.api.posts.model import AssetRow, PostRow
from dank.api.viewer.deps import viewer_store
from dank.config import Settings
from dank.web.app import assets_path
from tests.api.client import http_client
from tests.api.viewer.fakes import FakeViewerStore

NO_CACHE = "no-store, no-cache, must-revalidate, max-age=0"
CREATED = dt.datetime(2026, 10, 30, 12, 30, tzinfo=dt.UTC)


def post(post_id: str = "1", **values: str) -> PostRow:
    return PostRow(
        domain=values.get("domain", "politics.example"),
        post_id=post_id,
        url=f"https://politics.example/{post_id}",
        author=values.get("author", "Ada"),
        title=values.get("title", f"Title {post_id}"),
        html=values.get("html", "<p>Body text</p>"),
        created_at=CREATED,
        updated_at=CREATED,
        source="rss",
    )


def asset(post_id: str, local_path: pathlib.Path, kind: str) -> AssetRow:
    return AssetRow(post_id, "https://cdn.example/a", str(local_path), kind, 4)


def assert_html_page(response_headers: dict[str, str]) -> None:
    assert response_headers["content-type"] == "text/html; charset=utf-8"
    assert response_headers["cache-control"] == NO_CACHE
    assert response_headers["pragma"] == "no-cache"
    assert response_headers["expires"] == "0"


def pager_query(page: str, direction: str) -> dict[str, list[str]]:
    link = page.split(f'class="pager-{direction}" href="')[1].split('"')[0]

    return parse_qs(urlsplit(html.unescape(link)).query)


async def test_an_empty_index_renders_for_the_compose_healthcheck(
    client: AsyncClient,
) -> None:
    response = await client.get("/")

    assert response.status_code == 200
    assert_html_page(dict(response.headers))
    assert '<h1><a href="/">DANK</a></h1>' in response.text
    assert '<link rel="stylesheet" href="/static/app.css" />' in response.text


async def test_head_answers_without_a_body(client: AsyncClient) -> None:
    response = await client.head("/")

    assert (response.status_code, response.content) == (200, b"")
    assert_html_page(dict(response.headers))


async def test_the_index_shows_posts_with_links_and_a_summary(
    client: AsyncClient, store: FakeViewerStore,
) -> None:
    store.listed = [post(html="<p>Hello <b>there</b></p>", title="A & B")]

    response = await client.get("/")

    assert "A &amp; B" in response.text
    assert "Hello there" in response.text
    assert "2026-10-30 12:30 UTC" in response.text
    assert "/post?domain=politics.example&amp;post_id=1" in response.text
    assert 'href="https://politics.example/1"' in response.text


async def test_the_form_keeps_what_was_asked_for(client: AsyncClient) -> None:
    response = await client.get("/", params={
        "domain": "x.com", "account": "dense", "days_back": "7", "limit": "9",
    })

    assert 'value="x.com"' in response.text
    assert 'value="dense"' in response.text
    assert 'name="days_back"' in response.text
    assert 'name="limit" value="9"' in response.text


async def test_values_it_cant_read_fall_back_to_the_defaults(
    client: AsyncClient,
) -> None:
    response = await client.get("/", params={
        "limit": "many",
        "days_back": "-3",
        "cursor_created_at": "not a date",
        "cursor_direction": "sideways",
        "unknown": "ignored",
    })

    assert response.status_code == 200
    assert 'name="limit" value="50"' in response.text


async def test_pager_links_carry_the_filters_and_the_cursor(
    client: AsyncClient, store: FakeViewerStore,
) -> None:
    store.listed = [post("2"), post("1")]
    store.newer = True
    store.older = True

    response = await client.get("/", params={"domain": "x.com", "limit": "2"})

    previous = pager_query(response.text, "prev")
    following = pager_query(response.text, "next")

    assert previous["cursor_direction"] == ["prev"]
    assert previous["cursor_post_id"] == ["2"]
    assert following["cursor_direction"] == ["next"]
    assert following["cursor_post_id"] == ["1"]
    assert following["cursor_created_at"] == [CREATED.isoformat()]
    assert following["domain"] == ["x.com"]
    assert following["limit"] == ["2"]


async def test_the_first_page_has_no_previous_link(
    client: AsyncClient, store: FakeViewerStore,
) -> None:
    store.listed = [post()]

    response = await client.get("/")

    assert "pager-prev" not in response.text
    assert "pager-next" not in response.text


async def test_search_uses_its_own_results_and_has_no_pager(
    client: AsyncClient, store: FakeViewerStore,
) -> None:
    store.listed = [post("1", title="Listed")]
    store.searched = [post("2", title="Found")]
    store.newer = True
    store.older = True

    response = await client.get("/", params={"q": " space "})

    assert "Found" in response.text
    assert "Listed" not in response.text
    assert "pager-next" not in response.text
    assert "search-clear" in response.text


async def test_a_post_page_cleans_its_body(
    client: AsyncClient, store: FakeViewerStore,
) -> None:
    unsafe = '<p onclick="x()">Safe</p><script>bad()</script>'
    store.listed = [post(html=unsafe)]

    response = await client.get("/post", params={
        "domain": "politics.example", "post_id": "1",
    })

    assert response.status_code == 200
    assert_html_page(dict(response.headers))
    assert "<p>Safe</p>" in response.text
    assert "bad()" not in response.text
    assert "onclick" not in response.text


async def test_local_assets_are_shown_by_kind(
    client: AsyncClient,
    store: FakeViewerStore,
    settings: Settings,
) -> None:
    assets = assets_path(settings)
    assets.mkdir(parents=True)
    rows: list[AssetRow] = []

    for name, kind in (
        ("a b.png", "image/png"),
        ("clip.mp4", "video/mp4"),
        ("voice.mp3", "audio/mpeg"),
        ("paper.pdf", "application/pdf"),
    ):
        (assets / name).write_bytes(b"data")
        rows.append(asset("1", assets / name, kind))

    store.listed = [post()]
    store.asset_rows = {"1": rows}

    response = await client.get("/post", params={
        "domain": "politics.example", "post_id": "1",
    })

    assert '<img src="/assets/a%20b.png"' in response.text
    assert "<video controls>" in response.text
    assert 'src="/assets/clip.mp4"' in response.text
    assert "<audio controls>" in response.text
    assert 'href="/assets/paper.pdf"' in response.text


async def test_assets_outside_the_directory_are_left_out(
    client: AsyncClient,
    store: FakeViewerStore,
    tmp_path: pathlib.Path,
) -> None:
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"data")
    store.listed = [post()]
    store.asset_rows = {"1": [asset("1", outside, "image/png")]}

    response = await client.get("/post", params={
        "domain": "politics.example", "post_id": "1",
    })

    assert response.status_code == 200
    assert "outside.png" not in response.text


@pytest.mark.parametrize("params", [
    {},
    {"domain": "politics.example"},
    {"post_id": "1"},
    {"domain": "", "post_id": "1"},
])
async def test_a_post_needs_both_keys_and_says_so_in_plain_text(
    client: AsyncClient, params: dict[str, str],
) -> None:
    response = await client.get("/post", params=params)

    assert (response.status_code, response.text) == (
        400, "Missing post_id or domain",
    )
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


async def test_a_missing_post_says_so_in_plain_text(
    client: AsyncClient,
) -> None:
    response = await client.get("/post", params={
        "domain": "politics.example", "post_id": "missing",
    })

    assert (response.status_code, response.text) == (404, "Post not found")
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


async def test_a_database_failure_is_a_plain_uncached_503(
    app: FastAPI,
) -> None:
    class Broken(FakeViewerStore):
        async def posts(self, filters: object, since: object) -> list[PostRow]:
            raise RuntimeError("ClickHouse is down")

    app.dependency_overrides[viewer_store] = lambda: Broken()

    async with http_client(app, raise_app_exceptions=False) as client:
        response = await client.get("/")

    assert response.status_code == 503
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.headers["cache-control"] == "no-store"
    assert "unavailable" in response.text
