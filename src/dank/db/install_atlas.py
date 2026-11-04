"""Download the pinned Atlas binary and verify it.

This uses only the standard library, because the Dockerfile runs it before
installing dependencies. Atlas can't come from uv: PyPI has only its provider.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import platform
import sys
import tempfile
import urllib.request
from collections.abc import Callable

VERSION = "v1.3.0"
# Never the -latest links, which serve canary builds. These come from the
# .sha256 file beside each download and live here so that a changed download
# fails, rather than a checksum served from the same host matching it.
SHA256 = {
    "linux-amd64":
        "cfc773e5b4e845bc01d680390c174648938a7f88bf30e5a2c83ae85217c21587",
    "linux-arm64":
        "d2cfb1c71e58170bb4ddbe62107a38da4490f516ffa53a13abf57583819b6a40",
    "darwin-amd64":
        "4b4d8621332eabc3e6e7554d799a98ceef6376c4d8bbad6ed7832b49655e3480",
    "darwin-arm64":
        "47aaf7c295c7569c7eecfcbc53f02de862846ce1fbef16f1bd8ae98b03c3c68f",
}
MACHINES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}
DOWNLOAD_TIMEOUT_SECONDS = 300


class InstallError(Exception):
    pass


def platform_name(system: str, machine: str) -> str:
    name = f"{system.lower()}-{MACHINES.get(machine.lower(), machine)}"

    if name not in SHA256:
        raise InstallError(f"Atlas {VERSION} isn't pinned for {name}")

    return name


def download_url(name: str) -> str:
    return f"https://atlasbinaries.com/atlas/atlas-{name}-{VERSION}"


def _download(url: str) -> bytes:
    # The host rejects urllib's default user agent.
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "dank-install-atlas"},
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        ) as response:
            return response.read()
    except OSError as error:
        raise InstallError(f"Cannot download {url}: {error}") from error


def install(
    target: pathlib.Path,
    name: str,
    *,
    download: Callable[[str], bytes] = _download,
) -> None:
    data = download(download_url(name))

    if hashlib.sha256(data).hexdigest() != SHA256[name]:
        raise InstallError(
            f"The Atlas {VERSION} download for {name} failed verification",
        )

    target.parent.mkdir(parents=True, exist_ok=True)

    # Replace atomically, so a failed write can't leave a broken binary.
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as file:
        file.write(data)

    os.chmod(file.name, 0o755)
    os.replace(file.name, target)


def main() -> None:
    target = pathlib.Path(sys.argv[1])
    install(target, platform_name(platform.system(), platform.machine()))
    print(f"Installed Atlas {VERSION} at {target}")


if __name__ == "__main__":
    main()
