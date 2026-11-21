"""`scrape` collects the sources in use: config.toml's, or the table's."""

import pathlib
import re
from typing import Any

import pytest

from dank.config import Settings, SourceConfig
from dank.db.sources import NO_SOURCES, SourcesError
from dank.scrape import (
    __main__ as cli,
    runner,
)
from dank.sources.model import CompiledSources
from tests.sources.fakes import (
    OFF,
    ON,
    SAVED,
    FakeSourcesInUse,
    no_logging,
    write_config,
)


class Runs:
    """Records the settings each kind of run was started with."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.started: list[tuple[str, Settings]] = []
        self.options: dict[str, object] = {}

        for name in (
            "run_scrape", "run_youtube_metadata", "retry_youtube_thumbnails",
        ):
            monkeypatch.setattr(runner, name, self._recorder(name))

    def _recorder(self, name: str) -> Any:
        async def run(settings: Settings, **options: object) -> None:
            self.started.append((name, settings))
            self.options = options

        return run


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> Runs:
    return Runs(monkeypatch)


@pytest.fixture
def in_use(monkeypatch: pytest.MonkeyPatch) -> FakeSourcesInUse:
    in_use = FakeSourcesInUse(SAVED)
    monkeypatch.setattr(runner, "sources_in_use", in_use)
    monkeypatch.setattr(runner, "configure_logging", no_logging)

    return in_use


def test_scrape_collects_the_enabled_sources_in_use(
    tmp_path: pathlib.Path, runs: Runs, in_use: FakeSourcesInUse,
) -> None:
    runner.run_scrape_from_config(write_config(tmp_path))

    [(name, settings)] = runs.started

    assert (name, settings.sources) == ("run_scrape", (ON,))


def test_account_backfill_only_selects_requested_handles(
    tmp_path: pathlib.Path, runs: Runs, in_use: FakeSourcesInUse,
) -> None:
    channel_id = "UCBa659QWEk1AI4Tg--mrJ2A"
    x = SourceConfig("x.com", ("@WatcherGuru", "Barchart"), tags=("news",))
    youtube = SourceConfig(
        "youtube.com", ("WatcherGuru", channel_id, "another"),
        ("https://www.youtube.com/@watcherguru/shorts",
         "https://www.youtube.com/playlist?list=selected_playlist",
         "https://www.youtube.com/@other/videos"),
    )
    in_use.answer = CompiledSources((x, youtube, ON), (x, youtube, ON))

    runner.run_scrape_from_config(
        write_config(tmp_path),
        accounts=("@WatcherGuru", "watcherguru", channel_id),
    )
    [(name, settings)] = runs.started

    assert name == "run_scrape"
    assert settings.sources == (
        x._replace(accounts=("@WatcherGuru",)),
        youtube._replace(
            accounts=(), feed_urls=(
                "https://www.youtube.com/@watcherguru/shorts",
                "https://www.youtube.com/@WatcherGuru/videos",
                f"https://www.youtube.com/channel/{channel_id}/videos",
            ),
        ),
    )
    assert not settings.x.homepage
    assert settings.x.known_overlap == 0
    assert not settings.youtube.homepage
    assert not settings.youtube.hype
    assert not settings.youtube.news_tabs
    assert not settings.youtube.playlists
    assert runs.options["scoped"] is True


def test_account_selection_reports_disabled_and_missing_names(
    tmp_path: pathlib.Path, runs: Runs, in_use: FakeSourcesInUse,
) -> None:
    disabled = SourceConfig("x.com", ("disabled",))
    enabled = SourceConfig("x.com", ("enabled",))
    in_use.answer = CompiledSources((enabled, disabled), (enabled,))

    with pytest.raises(
        SourcesError, match="No enabled account matches: disabled, missing",
    ):
        runner.run_scrape_from_config(
            write_config(tmp_path),
            accounts=("enabled", "disabled", "missing"),
        )

    assert runs.started == []


def test_domains_select_among_the_sources_in_use(
    tmp_path: pathlib.Path, runs: Runs, in_use: FakeSourcesInUse,
) -> None:
    runner.run_scrape_from_config(
        write_config(tmp_path), domain_regex=re.compile("example"),
    )
    runner.run_scrape_from_config(
        write_config(tmp_path), domain_regex=re.compile("^off"),
    )

    assert [settings.sources for _, settings in runs.started] == [(ON,), ()]
    assert OFF not in runs.started[0][1].sources


@pytest.mark.parametrize(("options", "name"), [
    ({"x_posts": ("https://x.com/writer/status/100",)}, "run_scrape"),
    ({"youtube_metadata_only": True}, "run_youtube_metadata"),
    ({"retry_youtube_thumbnails_only": True}, "retry_youtube_thumbnails"),
])
def test_runs_that_name_their_own_posts_need_no_sources(
    tmp_path: pathlib.Path,
    runs: Runs,
    in_use: FakeSourcesInUse,
    options: dict[str, Any],
    name: str,
) -> None:
    in_use.answer = SourcesError(NO_SOURCES)

    runner.run_scrape_from_config(write_config(tmp_path), **options)

    assert [started for started, _ in runs.started] == [name]
    assert in_use.calls == 0


def test_one_x_post_is_collected_whatever_the_sources_are(
    tmp_path: pathlib.Path, runs: Runs, in_use: FakeSourcesInUse,
) -> None:
    runner.run_scrape_from_config(
        write_config(tmp_path), x_posts=("https://x.com/writer/status/100",),
    )

    assert runs.started[0][1].sources == (SourceConfig("x.com", ()),)


def test_scrape_says_why_it_has_no_sources(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: pathlib.Path,
    runs: Runs,
    in_use: FakeSourcesInUse,
) -> None:
    in_use.answer = SourcesError(NO_SOURCES)
    monkeypatch.setattr(
        "sys.argv", ["scrape", "--config", write_config(tmp_path)],
    )

    with pytest.raises(SystemExit) as exited:
        cli.main()

    assert exited.value.code == 1
    assert capsys.readouterr().err == f"scrape: {NO_SOURCES}\n"
    assert runs.started == []
