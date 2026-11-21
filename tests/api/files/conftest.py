"""A small reader build and data assets for the file routes."""

import pathlib

import pytest
from fastapi import FastAPI

from dank.api.app import create_app
from dank.api.files.static import FileRoots
from dank.config import Settings
from dank.web.assets import assets_path

INDEX = "<!doctype html><title>Reader</title>"
BUNDLE = "console.log('reader');"


@pytest.fixture
def roots(memory_path: pathlib.Path) -> FileRoots:
    reader = memory_path / "reader-dist"
    (reader / "bundle").mkdir(parents=True)
    (reader / "index.html").write_text(INDEX)
    (reader / "bundle" / "index-abc.js").write_text(BUNDLE)
    (reader / "icon.svg").write_text("<svg/>")

    return FileRoots(reader=reader)


@pytest.fixture
def settings(settings: Settings, memory_path: pathlib.Path) -> Settings:
    return settings._replace(data_dir=memory_path / "data")


@pytest.fixture
def assets(settings: Settings) -> pathlib.Path:
    directory = assets_path(settings)
    directory.mkdir(parents=True)

    return directory


@pytest.fixture(scope="module")
def file_app(api_settings: Settings) -> FastAPI:
    reader = api_settings.data_dir.parent / "reader-dist"

    return create_app(api_settings, roots=FileRoots(reader=reader))


@pytest.fixture
def app(file_app: FastAPI, roots: FileRoots, assets: pathlib.Path) -> FastAPI:
    # The routes keep their mount points while each test gets fresh files.
    return file_app
