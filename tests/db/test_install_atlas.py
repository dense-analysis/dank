import hashlib
import pathlib
import sys

import pytest

from dank.db import (
    __main__ as dank_db,
    install_atlas,
)
from dank.db.install_atlas import InstallError, platform_name


@pytest.mark.parametrize(
    ("system", "machine", "name"),
    [
        ("Linux", "x86_64", "linux-amd64"),
        ("Linux", "aarch64", "linux-arm64"),
        ("Darwin", "arm64", "darwin-arm64"),
        ("Darwin", "x86_64", "darwin-amd64"),
    ],
)
def test_platform_names_cover_docker_and_macos(
    system: str,
    machine: str,
    name: str,
) -> None:
    assert platform_name(system, machine) == name


def test_unpinned_platform_is_refused() -> None:
    with pytest.raises(InstallError, match="isn't pinned"):
        platform_name("Windows", "x86_64")


def test_download_is_a_pinned_version_rather_than_latest() -> None:
    url = install_atlas.download_url("linux-amd64")

    assert url.endswith(f"atlas-linux-amd64-{install_atlas.VERSION}")
    assert "latest" not in url
    assert url.startswith("https://")


def test_a_changed_download_is_never_installed(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "bin" / "atlas"

    with pytest.raises(InstallError, match="failed verification"):
        install_atlas.install(
            target,
            "linux-amd64",
            download=lambda _url: b"something else",
        )

    assert not target.exists()


def test_a_verified_download_is_installed_executable(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = b"pretend binary"
    monkeypatch.setitem(
        install_atlas.SHA256,
        "linux-amd64",
        hashlib.sha256(data).hexdigest(),
    )
    target = tmp_path / "bin" / "atlas"

    install_atlas.install(target, "linux-amd64", download=lambda _url: data)

    assert target.read_bytes() == data
    assert target.stat().st_mode & 0o111
    assert [path.name for path in target.parent.iterdir()] == ["atlas"]


def install_command(
    monkeypatch: pytest.MonkeyPatch, *args: str,
) -> list[pathlib.Path]:
    """Run `dank-db install-atlas`, recording where it would install."""
    installed: list[pathlib.Path] = []

    def install(target: pathlib.Path, _name: str) -> None:
        installed.append(target)

    monkeypatch.setattr(install_atlas, "install", install)
    monkeypatch.setattr("sys.argv", ["dank-db", "install-atlas", *args])

    with pytest.raises(SystemExit) as exited:
        dank_db.main()

    assert exited.value.code == 0

    return installed


def test_atlas_installs_into_this_checkout_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert install_command(monkeypatch) == [
        pathlib.Path(sys.prefix) / "bin" / "atlas",
    ]


def test_one_install_can_serve_every_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    assert install_command(monkeypatch, "--dir", str(tmp_path / "bin")) == [
        tmp_path / "bin" / "atlas",
    ]


def test_a_shared_directory_can_start_from_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    # As in ARGS='--dir=~/.local/bin', where the shell leaves ~ alone.
    monkeypatch.setenv("HOME", str(tmp_path))

    assert install_command(monkeypatch, "--dir=~/.local/bin") == [
        tmp_path / ".local" / "bin" / "atlas",
    ]
