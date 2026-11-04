"""Make is DANK's one interface for commands, so keep it complete."""

import pathlib
import re
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAKEFILE = (ROOT / "Makefile").read_text()
TARGETS = re.search(r"^\.PHONY: (.+)$", MAKEFILE, re.MULTILINE)


def _mentions(text: str, word: str) -> bool:
    # Hyphens count as part of a name, so test doesn't match test-postgres.
    return bool(re.search(rf"(?<![\w-]){re.escape(word)}(?![\w-])", text))


def _targets() -> list[str]:
    assert TARGETS is not None

    return [name for name in TARGETS[1].split() if name != "help"]


def test_every_command_has_a_make_target() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    missing = [
        script for script in pyproject["project"]["scripts"]
        if not _mentions(MAKEFILE, script)
    ]

    assert not missing


def test_every_target_is_in_make_help() -> None:
    help_text = MAKEFILE.split("\nhelp:\n", 1)[1].split("\n\n", 1)[0]
    missing = [name for name in _targets() if not _mentions(help_text, name)]

    assert not missing


def test_every_target_is_in_the_readme() -> None:
    readme = (ROOT / "README.md").read_text()
    missing = [
        name for name in _targets()
        if not _mentions(readme, f"make {name}")
    ]

    assert not missing
