"""Renders the legacy viewer's pages with Jinja2."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

from jinja2 import Environment, FileSystemLoader, select_autoescape

from dank.api.posts.model import AssetRow, PostRow
from dank.api.viewer.model import (
    IndexFilters,
    IndexPage,
    PostPage,
    days_back_label,
)
from dank.web.app import (
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)

TEMPLATES = Environment(
    loader=FileSystemLoader(pathlib.Path(__file__).parent / "templates"),
    autoescape=select_autoescape(("html", "xml")),
)


class ViewerPages:
    def __init__(self, assets_dir: pathlib.Path) -> None:
        self._assets_dir = assets_dir

    def index(self, filters: IndexFilters, page: IndexPage) -> str:
        return TEMPLATES.get_template("index.html").render(
            title="DANK Posts",
            posts=[
                self._post_view(post, page.assets.get(post.post_id, ()))
                for post in page.posts
            ],
            search_text=filters.search_text,
            domain_filter=filters.domain,
            account_filter=filters.account,
            days_back=filters.days_back,
            days_back_label=days_back_label(filters.days_back),
            limit=filters.limit,
            previous_link=_pager_link(filters, page, "prev"),
            next_link=_pager_link(filters, page, "next"),
        )

    def post(self, page: PostPage) -> str:
        post = page.post

        return TEMPLATES.get_template("post_detail.html").render(
            title=post.title or post.url,
            post={
                "title": post.title or post.url,
                "author": post.author,
                "created_at": _format_display_datetime(post.created_at),
                "source_link": post.url,
                "body_html": _sanitize_html(post.html),
            },
            assets=self._asset_views(page.assets),
        )

    def _post_view(
        self, post: PostRow, assets: Sequence[AssetRow],
    ) -> dict[str, Any]:
        return {
            "title": post.title or post.url,
            "author": post.author,
            "domain": post.domain,
            "created_at": _format_display_datetime(post.created_at),
            "summary": _summarize_html(post.html),
            "detail_link": "/post?" + urlencode(
                {"domain": post.domain, "post_id": post.post_id},
            ),
            "source_link": post.url,
            "assets": self._asset_views(assets),
        }

    def _asset_views(
        self, assets: Sequence[AssetRow],
    ) -> list[dict[str, str]]:
        return [
            view
            for view in (
                _asset_view(asset, assets_dir=self._assets_dir)
                for asset in assets
            )
            if view is not None
        ]


def _pager_link(
    filters: IndexFilters, page: IndexPage, direction: str,
) -> str:
    previous = direction == "prev"
    wanted = page.has_previous_page if previous else page.has_next_page

    if not page.posts or filters.search_text or not wanted:
        return ""

    cursor_post = page.posts[0] if previous else page.posts[-1]
    params = {
        "limit": str(filters.limit),
        "days_back": str(filters.days_back),
    }

    if filters.domain:
        params["domain"] = filters.domain

    if filters.account:
        params["account"] = filters.account

    params.update({
        "cursor_created_at": _cursor_datetime(cursor_post.created_at),
        "cursor_post_id": cursor_post.post_id,
        "cursor_direction": direction,
    })

    return "/?" + urlencode(params)


def _cursor_datetime(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)

    return value.isoformat()


def _format_display_datetime(value: dt.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(dt.UTC)

    return value.strftime("%Y-%m-%d %H:%M UTC")
