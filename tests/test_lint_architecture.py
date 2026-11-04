"""Verify the shell check rejects imports across pipeline boundaries."""

from __future__ import annotations

import pathlib
from subprocess import PIPE, Popen

import pytest

SCRIPT = (
    pathlib.Path("run-linters.sh")
    .read_text()
    .split(
        "# Keep API domain rules",
        1,
    )[0]
)


# Bind before the network guard; run only the local architecture checks.
def _check(path: pathlib.Path) -> tuple[int, str]:
    with Popen(["bash", "-c", SCRIPT], cwd=path, stdout=PIPE, stderr=PIPE,
               text=True) as process:
        _, stderr = process.communicate(timeout=10)

        return process.returncode, stderr


@pytest.mark.parametrize(
    "statement",
    [
        "from dank.training.runner import run_train",
        "import dank.training.decisions",
        "from dank import training",
    ],
)
def test_processing_cannot_import_training(
    tmp_path: pathlib.Path,
    statement: str,
) -> None:
    for module in ("scrape", "process", "training"):
        (tmp_path / "src/dank" / module).mkdir(parents=True)

    (tmp_path / "src/dank/process/runner.py").write_text(statement + "\n")
    returncode, stderr = _check(tmp_path)
    assert returncode != 0
    assert (
        "dank.process modules must not import dank.training" in stderr
    )


def test_independent_pipelines_can_use_shared_types(
    tmp_path: pathlib.Path,
) -> None:
    for module in ("scrape", "process", "training"):
        path = tmp_path / "src/dank" / module
        path.mkdir(parents=True)
        (path / "runner.py").write_text("from dank.model import Post\n")

    returncode, stderr = _check(tmp_path)
    assert returncode == 0, stderr
