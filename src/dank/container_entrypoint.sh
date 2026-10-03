#!/bin/sh
set -eu

if [ ! -f /run/dank/config.toml ]; then
    printf '%s\n' 'Missing configuration. Run make config and use docker compose.' >&2
    exit 1
fi

# Copy the private bind-mounted file so the application can run without root.
install -o dank -g dank -m 600 /run/dank/config.toml /app/config.toml
mkdir -p /app/data/assets /home/dank/.cache
chown dank:dank /app/data /app/data/assets /home/dank/.cache

exec gosu dank "$@"
