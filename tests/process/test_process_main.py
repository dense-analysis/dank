import sys
from unittest.mock import MagicMock

import pytest

from dank.process import __main__ as cli


@pytest.mark.parametrize("age", ["168h", "all"])
def test_cli_passes_scoped_reprocessing_options(
    monkeypatch: pytest.MonkeyPatch, age: str,
) -> None:
    run = MagicMock()
    monkeypatch.setattr(cli, "run_process_from_config", run)
    monkeypatch.setattr(sys, "argv", [
        "process", "--config", "test.toml", "--age", age,
        "--domains", r"^www\.theregister\.com$", "--reprocess",
    ])
    cli.main()

    run.assert_called_once()
    assert run.call_args.args == ("test.toml",)
    assert run.call_args.kwargs["age"] == age
    assert run.call_args.kwargs["reprocess"] is True
    regex = run.call_args.kwargs["domain_regex"]
    assert regex.search("www.theregister.com")
    assert not regex.search("other.test")


def test_cli_rejects_invalid_domain_regex_before_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = MagicMock()
    monkeypatch.setattr(cli, "run_process_from_config", run)
    monkeypatch.setattr(sys, "argv", ["process", "--domains", "["])

    with pytest.raises(SystemExit, match="2"):
        cli.main()

    run.assert_not_called()
