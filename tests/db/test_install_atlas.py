import hashlib
import pathlib

import pytest

from dank.db import install_atlas
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
