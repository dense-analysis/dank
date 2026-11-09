"""The reader and data assets, on Starlette's StaticFiles.

StaticFiles and its FileResponse already answer range and conditional
requests, which tests/api/files holds them to. Only the reader needs code of
its own, for its client-side routes and its entry page.
"""

import os
import pathlib
import stat
from typing import NamedTuple

from fastapi import FastAPI
from starlette.exceptions import HTTPException
from starlette.responses import Response
from starlette.staticfiles import PathLike, StaticFiles
from starlette.types import Scope

from dank.api.files.model import client_route, server_path
from dank.config import Settings
from dank.web.app import assets_path

ROOT = pathlib.Path(__file__).resolve().parents[4]


class FileRoots(NamedTuple):
    """Folders from the checkout, rather than from configuration."""

    reader: pathlib.Path = ROOT / "reader" / "dist"


DEFAULT_ROOTS = FileRoots()


class ReaderFiles(StaticFiles):
    """The built app: its files, or index.html for its client-side routes."""

    async def check_config(self) -> None:
        """Let a checkout with no reader build answer 404, not fail."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        # As the fallback, this also gets unmatched API paths, whatever the
        # method. They keep their 404 rather than a 405 or the app.
        if server_path(path):
            raise HTTPException(status_code=404)

        return await super().get_response(path, scope)

    def lookup_path(self, path: str) -> tuple[str, os.stat_result | None]:
        full_path, found = super().lookup_path(path)

        if (found and stat.S_ISREG(found.st_mode)) or not client_route(path):
            return full_path, found

        return super().lookup_path("index.html")

    def file_response(
        self,
        full_path: PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(
            full_path, stat_result, scope, status_code,
        )

        # Set here because a 304 has no content type for ApiHeaders to catch.
        if os.path.basename(full_path) == "index.html":
            response.headers["cache-control"] = "no-store"

        return response


def mount_files(app: FastAPI, settings: Settings, roots: FileRoots) -> None:
    # The lifespan creates this directory.
    assets = StaticFiles(directory=assets_path(settings), check_dir=False)
    app.mount("/assets", assets)
    # The fallback for any path no route or mount matched, so the reader can
    # live at / without shadowing routes added after it.
    app.router.default = ReaderFiles(directory=roots.reader, check_dir=False)
