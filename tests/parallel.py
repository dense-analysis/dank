"""Route pytest's normal command line through the project's forkserver."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Generator
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import pytest

_SHARD_BY_DIRECTORY = {"api": "reader", "web": "reader", "scrape": "scrape"}
_SHARD_NAMES = {
    "reader": "API and reader tests",
    "scrape": "Scraping tests",
    "other": "Other tests",
}
# These options need pytest to own the run or handle it without running tests.
_NATIVE_OPTIONS = (
    "help", "version", "markers", "collectonly", "showfixtures",
    "show_fixtures_per_test", "setupplan", "setuponly", "setupshow",
    "cacheshow", "cacheclear", "xmlpath", "lf", "failedfirst", "newfirst",
    "stepwise", "stepwise_skip", "stepwise_reset", "maxfail", "looponfail",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--dank-test-shard", choices=(*_SHARD_NAMES, "all"))


def pytest_ignore_collect(
    collection_path: Path, config: pytest.Config,
) -> bool | None:
    shard = config.getoption("dank_test_shard")

    if (
        shard in (None, "all")
        or collection_path.parent != config.rootpath / "tests"
    ):
        return None

    # Let pytest apply its regular discovery rules inside the assigned paths.
    if _SHARD_BY_DIRECTORY.get(collection_path.name, "other") != shard:
        return True

    return None


@pytest.hookimpl(wrapper=True)
def pytest_collection(session: pytest.Session) -> Generator[None]:
    try:
        return (yield)
    except pytest.UsageError as error:
        if session.config.getoption("dank_test_shard") is None:
            raise

        # Report bad selectors over pytest-fast's collection-error channel.
        session.config.hook.pytest_collectreport(report=pytest.CollectReport(
            nodeid="", outcome="failed", longrepr=str(error), result=[],
        ))


def _run_forkserver(
    shard: str, workers: int, environment: dict[str, str], output: Path, *,
    full_report: bool,
) -> subprocess.CompletedProcess[str]:
    options = f"{environment['PYTEST_ADDOPTS']} --dank-test-shard={shard}"
    command = [
        sys.executable, "-m", "pytest_fast", "--workers", str(workers),
        "--dump", str(output),
    ]

    if full_report:
        command.append("--full-report")

    return subprocess.run(
        command, env={**environment, "PYTEST_ADDOPTS": options},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        check=False,
    )


def _workers(config: pytest.Config) -> int:
    requested = config.getoption("numprocesses", None)

    if isinstance(requested, int):
        workers = requested
    elif requested in ("auto", "logical"):
        workers = config.hook.pytest_xdist_auto_num_workers(config=config)
    else:
        workers = os.process_cpu_count() or 1

    cap = config.getoption("maxprocesses", None)

    return min(workers, cap) if cap else workers


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config: pytest.Config) -> int | None:
    if config.getoption("trace") or config.getoption("capture") == "no":
        config.option.numprocesses = 0
        config.option.tx = []
        config.option.dist = "no"

        return None

    if (
        config.getoption("usepdb")
        or config.getoption("numprocesses", None) == 0
        or config.getoption("cov_source", None) is not None
        or any(
            config.getoption(name, default=False) for name in _NATIVE_OPTIONS
        )
    ):
        return None

    # Forward the already validated CLI and inherited pytest options.
    environment = os.environ.copy()
    environment["PYTEST_ADDOPTS"] = (
        f"{environment.get('PYTEST_ADDOPTS', '')} "
        f"{shlex.join(config.invocation_params.args)}"
    )

    if mark := config.getoption("markexpr"):
        environment["PYTEST_FAST_MARK"] = mark

    workers = _workers(config)
    groups = (("all", workers),)

    if (
        workers >= 12
        and config.args == config.getini("testpaths")
        and not config.getoption("keyword")
        and not environment.get("PYTEST_FAST_MARK")
        and not environment.get("PYTEST_FAST_ADDRESS")
    ):
        scrape_workers = workers // 3
        other_workers = workers // 4
        groups = (
            ("scrape", scrape_workers),
            ("reader", workers - scrape_workers - other_workers),
            ("other", other_workers),
        )

    return _run_groups(
        groups, environment,
        full_report=config.getoption("durations") is not None,
    )


def _run_groups(
    groups: tuple[tuple[str, int], ...], environment: dict[str, str], *,
    full_report: bool,
) -> int:
    with (
        tempfile.TemporaryDirectory(prefix="dank-pytest-") as directory,
        ThreadPoolExecutor(len(groups)) as pool,
    ):
        jobs: list[
            tuple[str, Path, Future[subprocess.CompletedProcess[str]]]
        ] = []

        for shard, workers in groups:
            output = Path(directory) / f"{shard}.json"
            job = pool.submit(
                _run_forkserver, shard, workers, environment, output,
                full_report=full_report,
            )
            jobs.append((shard, output, job))

        status = pytest.ExitCode.OK
        ran_tests = False

        for shard, output, job in jobs:
            result = job.result()

            if shard != "all":
                print(_SHARD_NAMES[shard])

            print(result.stdout.rstrip(), flush=True)

            if result.returncode:
                status = result.returncode
            elif environment.get("PYTEST_FAST_ADDRESS") or json.loads(
                output.read_text(),
            ):
                ran_tests = True

    if status:
        return status

    if ran_tests:
        return pytest.ExitCode.OK

    return pytest.ExitCode.NO_TESTS_COLLECTED
