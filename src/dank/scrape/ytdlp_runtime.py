"""Discover JavaScript runtimes for yt-dlp's API and command line."""

from os import pathsep
from shutil import which

COMMON_RUNTIME_PATH = pathsep.join((
    "/usr/local/bin", "/usr/bin", "/bin", "/opt/homebrew/bin",
))


def js_runtime_options() -> dict[str, dict[str, str]]:
    # Keep yt-dlp's default Deno lookup when we have no explicit path.
    runtimes: dict[str, dict[str, str]] = {"deno": {}}

    for name in ("deno", "node"):
        executable = which(name) or which(name, path=COMMON_RUNTIME_PATH)

        if executable:
            runtimes[name] = {"path": executable}

    return runtimes


def js_runtime_arguments() -> list[str]:
    arguments: list[str] = []

    for name, options in js_runtime_options().items():
        if executable := options.get("path"):
            arguments.extend(["--js-runtimes", f"{name}:{executable}"])

    return arguments
