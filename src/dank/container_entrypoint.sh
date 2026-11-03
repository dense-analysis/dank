#!/bin/sh
set -eu

if [ ! -f /run/dank/config.toml ] || [ ! -f /run/secrets/postgres_password ]; then
    printf '%s\n' 'Missing configuration. Run make config and use docker compose.' >&2
    exit 1
fi

# Copy the private bind-mounted files so the application can run without root.
install -o dank -g dank -m 600 /run/dank/config.toml /app/config.toml
install -o dank -g dank -m 600 /run/secrets/postgres_password /app/postgres-password
mkdir -p /app/data/assets /home/dank/.cache
chown dank:dank /app/data /app/data/assets /home/dank/.cache

exec gosu dank "$@"
