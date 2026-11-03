import pathlib

import pytest

from dank.config import (
    ConfigError,
    PostgresSettings,
    Settings,
    load_settings,
    read_postgres_password,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
SECRET = "correct-horse-battery-staple"


def _load(tmp_path: pathlib.Path, config: str) -> Settings:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(config)

    return load_settings(path)


def test_missing_section_uses_the_compose_service_defaults(
    tmp_path: pathlib.Path,
) -> None:
    assert _load(tmp_path, "").postgres == PostgresSettings(
        host="postgres",
        port=5432,
        database="dank",
        user="dank",
        password_file=pathlib.Path("postgres-password"),
    )


def test_defaults_match_the_compose_service() -> None:
    compose = (ROOT / "compose.yaml").read_text()
    defaults = PostgresSettings()

    assert f"\n  {defaults.host}:\n" in compose
    assert f"POSTGRES_DB: {defaults.database}\n" in compose
    assert f"POSTGRES_USER: {defaults.user}\n" in compose


def test_container_copies_the_secret_to_the_default_password_file() -> None:
    entrypoint = (ROOT / "src/dank/container_entrypoint.sh").read_text()
    copied_to = f"/app/{PostgresSettings().password_file}"

    assert f"/run/secrets/postgres_password {copied_to}\n" in entrypoint


def test_docker_template_matches_the_defaults() -> None:
    settings = load_settings(ROOT / "config.example.toml")

    assert settings.postgres == PostgresSettings()


def test_native_template_points_at_a_local_server() -> None:
    settings = load_settings(ROOT / "config.native.example.toml")

    assert settings.postgres == PostgresSettings(host="localhost")


def test_section_overrides_every_field(tmp_path: pathlib.Path) -> None:
    settings = _load(
        tmp_path,
        '[postgres]\n'
        'host = "db.test"\n'
        'port = 6543\n'
        'database = "dank_my_task"\n'
        'user = "reader"\n'
        'password_file = "../main/postgres-password"\n',
    )

    assert settings.postgres == PostgresSettings(
        host="db.test",
        port=6543,
        database="dank_my_task",
        user="reader",
        password_file=pathlib.Path("../main/postgres-password"),
    )


def test_loading_settings_does_not_need_the_password_file(
    tmp_path: pathlib.Path,
) -> None:
    missing = tmp_path / "missing"
    settings = _load(tmp_path, f'[postgres]\npassword_file = "{missing}"\n')

    assert settings.postgres.password_file == missing


def test_names_may_use_the_full_postgres_identifier_length(
    tmp_path: pathlib.Path,
) -> None:
    name = "d" * 63
    settings = _load(tmp_path, f'[postgres]\ndatabase = "{name}"\n')

    assert settings.postgres.database == name


@pytest.mark.parametrize(
    ("config", "field"),
    [
        ('postgres = "localhost"\n', "postgres must be a table"),
        ('[postgres]\nhost = ""\n', "postgres.host"),
        ("[postgres]\nhost = 5\n", "postgres.host"),
        ('[postgres]\nhost = " db.test"\n', "postgres.host"),
        ('[postgres]\nport = "5432"\n', "postgres.port"),
        ("[postgres]\nport = true\n", "postgres.port"),
        ("[postgres]\nport = 0\n", "postgres.port"),
        ("[postgres]\nport = 65536\n", "postgres.port"),
        ('[postgres]\ndatabase = ""\n', "postgres.database"),
        ('[postgres]\ndatabase = "dank\\n"\n', "postgres.database"),
        # Postgres would silently truncate these to 63 bytes.
        (f'[postgres]\ndatabase = "{"d" * 64}"\n', "postgres.database"),
        ('[postgres]\nuser = ""\n', "postgres.user"),
        (f'[postgres]\nuser = "{"é" * 32}"\n', "postgres.user"),
        ('[postgres]\npassword_file = ""\n', "postgres.password_file"),
        ("[postgres]\npassword_file = 5\n", "postgres.password_file"),
        ('[postgres]\nhostname = "db.test"\n', "postgres accepts only"),
    ],
)
def test_invalid_settings_are_rejected(
    *, tmp_path: pathlib.Path, config: str, field: str,
) -> None:
    with pytest.raises(ConfigError, match=field):
        _load(tmp_path, config)


def test_inline_password_is_rejected_without_repeating_it(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(ConfigError, match="password_file") as caught:
        _load(tmp_path, f'[postgres]\npassword = "{SECRET}"\n')

    assert SECRET not in str(caught.value)


def test_settings_hold_the_password_path_but_not_the_password(
    tmp_path: pathlib.Path,
) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_text(SECRET)
    settings = _load(
        tmp_path, f'[postgres]\npassword_file = "{password_file}"\n',
    )

    assert read_postgres_password(settings.postgres) == SECRET
    assert SECRET not in repr(settings)


def test_password_strips_trailing_newlines_like_the_image_entrypoint(
    tmp_path: pathlib.Path,
) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_bytes(b" pass word \r\n\n")

    password = read_postgres_password(
        PostgresSettings(password_file=password_file),
    )

    assert password == " pass word \r"


@pytest.mark.parametrize("contents", [b"", b"\n\n"])
def test_empty_password_file_is_rejected(
    *, tmp_path: pathlib.Path, contents: bytes,
) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_bytes(contents)

    with pytest.raises(ConfigError, match="is empty"):
        read_postgres_password(PostgresSettings(password_file=password_file))


def test_unreadable_password_file_names_the_path(
    tmp_path: pathlib.Path,
) -> None:
    missing = tmp_path / "missing"

    with pytest.raises(ConfigError, match=str(missing)):
        read_postgres_password(PostgresSettings(password_file=missing))


def test_undecodable_password_is_not_shown(tmp_path: pathlib.Path) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_bytes(SECRET.encode() + b"\xff")

    with pytest.raises(ConfigError, match="UTF-8") as caught:
        read_postgres_password(PostgresSettings(password_file=password_file))

    assert SECRET not in str(caught.value)
    assert "xff" not in str(caught.value)
    assert caught.value.__suppress_context__
