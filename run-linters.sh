#!/usr/bin/env bash

set -eu

# Complain when scraping code is used in processing.
if grep -qrI 'dank.scrape' src/dank/process/; then
    echo 'dank.scrape modules should not be imported in dank.process modules!' 1>&2
    exit 1
fi

# Complain when processing code is used in scraping.
if grep -qrI 'dank.process' src/dank/scrape/; then
    echo 'dank.process modules should not be imported in dank.scrape modules!' 1>&2
    exit 1
fi


if grep  -qrI 'dank.storage.clickhouse' src/dank/process/ --exclude 'runner.py'; then
    echo 'dank.process code other than runner.py should not use clickhouse code!' 1>&2
    exit 1
fi

if grep -rI aiohttp src/dank/process/; then
    echo 'dank.process code should not be making HTTP requests!' 1>&2
    exit 1
fi

status=0

# Run Pyright
set -o pipefail

# Force colors through the pipe below if the terminal is interactive.
if [ -t 1 ]; then
    export FORCE_COLOR=1
fi

# We filter out noisy lines from Pyright AI agents don't need to read.
uv run pyright | grep -v 'new pyright\|PYRIGHT_PYTHON_FORCE_VERSION\|^$\|errors,.*warnings,.*informations' || true

if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    status=1
fi

set +o pipefail

# Run ruff and auto-fix errors.
uv run ruff check -q --fix . || status=1

if ! ((status)); then
    echo "All checks passed!"
fi

exit $status
