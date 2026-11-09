import pathlib

import pytest

from dank.config import ConfigError, Settings, load_settings

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(tmp_path: pathlib.Path, config: str) -> Settings:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(config)

    return load_settings(path)


def test_sources_come_from_config_unless_switched(
    tmp_path: pathlib.Path,
) -> None:
    assert _load(tmp_path, "").sources_in_database is False


@pytest.mark.parametrize("value", [True, False])
def test_the_switch_is_a_top_level_boolean(
    tmp_path: pathlib.Path, *, value: bool,
) -> None:
    settings = _load(
        tmp_path, f"sources_in_database = {str(value).lower()}\n",
    )

    assert settings.sources_in_database is value


@pytest.mark.parametrize("value", ['"true"', "1", "[]"])
def test_the_switch_must_be_a_boolean(
    tmp_path: pathlib.Path, value: str,
) -> None:
    with pytest.raises(
        ConfigError, match="sources_in_database must be a boolean",
    ):
        _load(tmp_path, f"sources_in_database = {value}\n")


def test_the_switch_needs_postgres(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ConfigError, match="sources_in_database needs"):
        _load(
            tmp_path,
            "sources_in_database = true\n[postgres]\nenabled = false\n",
        )


def test_the_switch_keeps_the_sources_config_lists(
    tmp_path: pathlib.Path,
) -> None:
    # They are ignored while the switch is on, but `make sources-import`
    # still reads them.
    settings = _load(
        tmp_path, 'sources_in_database = true\nsources = ["example.com"]\n',
    )

    assert [source.domain for source in settings.sources] == ["example.com"]


@pytest.mark.parametrize(
    "name", ["config.example.toml", "config.native.example.toml"],
)
def test_the_example_configs_leave_the_switch_off(name: str) -> None:
    assert load_settings(ROOT / name).sources_in_database is False
