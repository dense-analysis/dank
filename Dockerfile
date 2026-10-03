# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.10.0 AS uv
FROM node:22-bookworm-slim AS node
FROM python:3.13-slim-bookworm

COPY --from=uv /uv /uvx /usr/local/bin/
COPY --from=node /usr/local/bin/node /usr/local/bin/node

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates chromium ffmpeg fonts-liberation gosu tini \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 1000 dank \
    && mkdir -p /app/data/assets /home/dank/.cache \
    && chown -R dank:dank /app/data /home/dank/.cache

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY static ./static
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

COPY --chmod=755 src/dank/container_entrypoint.sh /usr/local/bin/dank-entrypoint
COPY --chmod=755 src/dank/container_chromium.sh /usr/local/bin/dank-chromium

EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/dank-entrypoint"]
CMD ["web", "--host", "0.0.0.0", "--no-reload"]
