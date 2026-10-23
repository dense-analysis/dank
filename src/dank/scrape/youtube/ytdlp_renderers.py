"""Retain homepage hierarchy before yt-dlp flattens its listing responses."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NamedTuple, cast

from .payloads import (
    POST_RENDERERS,
    SHELF_RENDERERS,
    VIDEO_RENDERERS,
    as_dict,
    excluded_home_section,
    text,
)


class HomeRenderer(NamedTuple):
    key: str
    raw: dict[str, Any]
    shelf: str
    shelves: tuple[str, ...] = ()


class ShelfContext(NamedTuple):
    shelves: tuple[str, ...] = ()
    excluded: bool = False


ROOT_CONTEXT = ShelfContext()


class RendererLimitReached(RuntimeError):
    pass


class UnknownHomeContinuation(RuntimeError):
    pass


class HomeResponseCapture:
    def __init__(
        self, emit: Callable[[dict[str, Any]], None], max_entries: int,
    ) -> None:
        self.emit = emit
        self.max_entries = max_entries
        self.entries = 0
        self.continuations: dict[str, ShelfContext] = {}

    def continuation_context(self, token: str) -> ShelfContext:
        context = self.continuations.get(token)

        if context is None:
            # Never guess that a heading-less continuation is an ordinary grid.
            raise UnknownHomeContinuation

        return context

    def observe(
        self, value: object, context: ShelfContext = ROOT_CONTEXT,
    ) -> None:
        if isinstance(value, list):
            for child in cast(list[object], value):
                self.observe(child, context)

            return

        for key, child in as_dict(value).items():
            data = as_dict(child)
            shelves = context.shelves

            if key in SHELF_RENDERERS and (heading := text(data.get("title"))):
                shelves = (*shelves, heading)

            nested = ShelfContext(
                shelves, context.excluded or excluded_home_section(key, data),
            )

            if key in {"continuationCommand", "nextContinuationData",
                       "reloadContinuationData"}:
                token = data.get("token") or data.get("continuation")

                if isinstance(token, str) and token:
                    previous = self.continuations.get(token)
                    # Conflicting scope is unsafe to reinterpret as allowed.
                    self.continuations[token] = (
                        ShelfContext(shelves=shelves, excluded=True)
                        if previous is not None and previous != nested
                        else nested
                    )

            if key in VIDEO_RENDERERS | POST_RENDERERS:
                if not nested.excluded:
                    if self.entries >= self.max_entries:
                        raise RendererLimitReached

                    self.emit({
                        "event": "renderer", "key": key, "raw": data,
                        "shelf": shelves[-1] if shelves else "",
                        "shelves": list(shelves),
                    })
                    self.entries += 1

                continue

            self.observe(child, nested)


def install_home_capture(
    provider: Any, emit: Callable[[dict[str, Any]], None], max_entries: int,
) -> None:
    # The pinned extractor owns requests/pagination; observe its raw responses.
    # Upstream implementation:
    # https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/youtube/_tab.py
    from yt_dlp.extractor.youtube import YoutubeTabIE  # type: ignore

    capture = HomeResponseCapture(emit, max_entries)

    class HomeTabIE(YoutubeTabIE):
        @classmethod
        def ie_key(cls) -> str:
            return "YoutubeTab"

        def _extract_data(self, *args: Any, **kwargs: Any) -> Any:
            base: Any = super()
            result: Any = base._extract_data(*args, **kwargs)
            capture.observe(result[0])

            return result

        def _extract_response(self, *args: Any, **kwargs: Any) -> Any:
            query = kwargs.get("query", args[1] if len(args) > 1 else {})
            token = as_dict(query).get("continuation")
            context = (
                capture.continuation_context(token)
                if isinstance(token, str) else None
            )
            base: Any = super()
            response: Any = base._extract_response(*args, **kwargs)

            if context is not None:
                capture.observe(response, context)

            return response

    provider.add_info_extractor(HomeTabIE(provider))
