"""A small reader build and data assets for the file routes."""

import pathlib

import pytest
from fastapi import FastAPI

from dank.api.app import create_app
from dank.api.files.static import FileRoots
from dank.config import Settings
from dank.web.app import assets_path

INDEX = "<!doctype html><title>Reader</title>"
BUNDLE = "console.log('reader');"


@pytest.fixture
def roots(tmp_path: pathlib.Path) -> FileRoots:
    reader = tmp_path / "dist"
    (reader / "bundle").mkdir(parents=True)
    (reader / "index.html").write_text(INDEX)
    (reader / "bundle" / "index-abc.js").write_text(BUNDLE)
    (reader / "icon.svg").write_text("<svg/>")

    return FileRoots(reader=reader)


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    directory = assets_path(settings)
    directory.mkdir(parents=True)

    return directory


@pytest.fixture
def app(
    settings: Settings, roots: FileRoots, assets: pathlib.Path,
) -> FastAPI:
    return create_app(settings, roots=roots)
