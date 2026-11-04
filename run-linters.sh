#!/usr/bin/env bash

set -eu

# Collection, processing and training own separate execution pipelines.
for module in scrape process training; do
    for dependency in scrape process training; do
        if [ "$module" = "$dependency" ]; then
            continue
        fi

        if rg --quiet --glob '*.py' \
            "dank[.]$dependency|from dank import .*\\b$dependency\\b" "src/dank/$module/"; then
            echo "dank.$module modules must not import dank.$dependency modules!" >&2
            exit 1
        fi
    done

    if rg --quiet --glob '*.py' --glob '!runner.py' \
        'dank[.]storage[.]clickhouse' "src/dank/$module/"; then
        echo "Only dank.$module.runner may access ClickHouse in dank.$module!" >&2
        exit 1
    fi
done

if grep -rI aiohttp src/dank/process/; then
    echo 'dank.process code should not be making HTTP requests!' 1>&2
    exit 1
fi

# Keep API domain rules and use cases free of frameworks and drivers.
if grep -rIlE '^(from|import) (fastapi|starlette|pydantic|sqlalchemy|clickhouse_connect|aiohttp|dank\.storage|dank\.web)' \
    --include model.py --include service.py src/dank/api/; then
    echo 'dank.api model.py and service.py must not import frameworks or drivers!' 1>&2
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
set +e
uv run pyright | grep -v 'new pyright\|PYRIGHT_PYTHON_FORCE_VERSION\|^$\|errors,.*warnings,.*informations'
pyright_status=${PIPESTATUS[0]}
set -e

if [ "$pyright_status" -ne 0 ]; then
    status=1
fi

set +o pipefail

# Run ruff and auto-fix errors.
uv run ruff check -q --fix . || status=1

if ! ((status)); then
    echo "All checks passed!"
fi

exit $status
