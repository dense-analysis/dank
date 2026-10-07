from __future__ import annotations

import pathlib
from unittest.mock import Mock

import pytest

from dank import runtime


def test_cgroup_v2_uses_tightest_visible_parent_limit(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "cgroup"
    child = root / "group/child"
    child.mkdir(parents=True)
    (root / "cpu.max").write_text("200000 100000")
    (root / "memory.max").write_text(str(1024 ** 3))
    (child / "cpu.max").write_text("50000 100000")
    (child / "memory.max").write_text(str(2 * 1024 ** 3))
    original = runtime._read  # pyright: ignore[reportPrivateUsage]

    def read(path: pathlib.Path) -> str:
        if str(path) == "/proc/self/cgroup":
            return "0::/group/child"

        return original(path)

    monkeypatch.setattr(runtime, "_read", read)
    assert runtime.cgroup_limits(root) == (0.5, 1024 ** 3)


def test_cgroup_v1_and_unlimited_memory(tmp_path: pathlib.Path) -> None:
    cpu = tmp_path / "cpu"
    memory = tmp_path / "memory"
    cpu.mkdir()
    memory.mkdir()
    (cpu / "cpu.cfs_quota_us").write_text("150000")
    (cpu / "cpu.cfs_period_us").write_text("100000")
    (memory / "memory.limit_in_bytes").write_text(str(2 ** 63 - 1))
    assert runtime.cgroup_limits(tmp_path) == (1.5, None)


@pytest.mark.parametrize("quota", ["max 100000", "broken", "10000 0", "-1 10"])
def test_unknown_or_unlimited_cgroups_do_not_invent_limits(
    tmp_path: pathlib.Path, quota: str,
) -> None:
    (tmp_path / "cpu.max").write_text(quota)
    (tmp_path / "memory.max").write_text("max")
    assert runtime.cgroup_limits(tmp_path) == (None, None)


def test_runtime_applies_capacity_limits_and_stable_code_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime.os, "process_cpu_count", lambda: 8)
    monkeypatch.setattr(runtime, "_physical_memory", lambda: 8 * 1024 ** 3)
    monkeypatch.setattr(
        runtime, "cgroup_limits", Mock(return_value=(1.5, 2 * 1024 ** 3)),
    )
    first = runtime.runtime_info()
    second = runtime.runtime_info()
    assert first.cpu_limit == 1.5
    assert first.memory_limit_bytes == 2 * 1024 ** 3
    assert first.code_version == second.code_version
    assert len(first.code_version) == 16


def test_macos_memory_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime.os, "sysconf", Mock(side_effect=ValueError))
    monkeypatch.setattr(runtime.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        runtime.subprocess, "check_output", Mock(return_value="8589934592"),
    )
    memory = runtime._physical_memory()  # pyright: ignore[reportPrivateUsage]
    assert memory == 8 * 1024 ** 3
