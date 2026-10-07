from __future__ import annotations

import hashlib
import os
import pathlib
import platform
import subprocess
from typing import NamedTuple


class RuntimeInfo(NamedTuple):
    runtime: str
    platform: str
    python_version: str
    cpu_limit: float
    memory_limit_bytes: int | None
    code_version: str


def _read(path: pathlib.Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def _positive_int(value: str) -> int | None:
    try:
        number = int(value)

        return number if 0 < number < 2 ** 60 else None
    except ValueError:
        return None


def _cgroup_paths(root: pathlib.Path) -> list[pathlib.Path]:
    paths = [root]
    membership = _read(pathlib.Path("/proc/self/cgroup"))

    for line in membership.splitlines():
        if line.startswith("0::"):
            relative = line[3:].lstrip("/")

            if ".." not in pathlib.PurePosixPath(relative).parts:
                child = root / relative
                paths.extend([child, *child.parents])

    return list(dict.fromkeys(paths))


def cgroup_limits(root: pathlib.Path) -> tuple[float | None, int | None]:
    # Inspect visible ancestors as nested cgroups inherit their limits.
    cpu: list[float] = []
    memory: list[int] = []

    for directory in _cgroup_paths(root):
        if directory != root and root not in directory.parents:
            continue

        values = _read(directory / "cpu.max").split()

        if len(values) == 2 and values[0] != "max":
            try:
                quota, period = map(int, values)

                if quota > 0 and period > 0:
                    cpu.append(quota / period)
            except ValueError:
                pass

        limit = _positive_int(_read(directory / "memory.max"))

        if limit:
            memory.append(limit)

    # Older Docker installations can still expose cgroup v1.
    quota = _positive_int(_read(root / "cpu/cpu.cfs_quota_us"))
    period = _positive_int(_read(root / "cpu/cpu.cfs_period_us"))

    if quota and period:
        cpu.append(quota / period)

    limit = _positive_int(_read(root / "memory/memory.limit_in_bytes"))

    if limit:
        memory.append(limit)

    return min(cpu) if cpu else None, min(memory) if memory else None


def _physical_memory() -> int | None:
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(
            os.sysconf("SC_PHYS_PAGES"),
        )
    except (OSError, ValueError):
        if platform.system() == "Darwin":
            try:
                return _positive_int(subprocess.check_output(
                    ["/usr/sbin/sysctl", "-n", "hw.memsize"],
                    text=True, timeout=2,
                ).strip())
            except (OSError, subprocess.SubprocessError):
                pass

    return None


def runtime_info() -> RuntimeInfo:
    cpu = float(os.process_cpu_count() or os.cpu_count() or 1)
    memory = _physical_memory()
    quota, memory_cap = cgroup_limits(pathlib.Path("/sys/fs/cgroup"))

    if quota is not None:
        cpu = min(cpu, quota)

    if memory_cap is not None:
        memory = min(memory, memory_cap) if memory else memory_cap

    package = pathlib.Path(__file__).parent
    digest = hashlib.sha256()

    for path in sorted(package.rglob("*.py")):
        digest.update(str(path.relative_to(package)).encode())
        digest.update(path.read_bytes())

    lock = package.parent.parent / "uv.lock"

    if lock.is_file():
        digest.update(lock.read_bytes())

    container = any(pathlib.Path(path).exists() for path in (
        "/.dockerenv", "/run/.containerenv",
    ))

    return RuntimeInfo(
        "container" if container else "native",
        f"{platform.system()} {platform.machine()}",
        platform.python_version(), cpu, memory, digest.hexdigest()[:16],
    )
