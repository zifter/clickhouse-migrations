# ---- build stage: build a wheel from the source tree ----
FROM python:3.13-slim AS build

WORKDIR /src
COPY . .

# The version is normally derived from git tags by setuptools-scm. The build
# context excludes .git (see .dockerignore), so CI passes the version here; a
# plain `docker build` falls back to the fallback_version from pyproject.toml.
ARG SETUPTOOLS_SCM_PRETEND_VERSION=""
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

RUN pip install --no-cache-dir build && python -m build --wheel --outdir /dist

# ---- runtime stage: install just the wheel + runtime deps ----
FROM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/zifter/clickhouse-migrations"
LABEL org.opencontainers.image.description="Simple file-based schema migrations for ClickHouse"
LABEL org.opencontainers.image.licenses="MIT"

COPY --from=build /dist/*.whl /tmp/
# Install with the [connect] extra so BOTH drivers ship in the image: the native
# clickhouse-driver (default) and the official HTTP clickhouse-connect driver
# (selected at runtime with --driver clickhouse-connect).
RUN pip install --no-cache-dir "$(echo /tmp/*.whl)[connect]" && rm -rf /tmp/*.whl

# Mount your migrations here (e.g. `-v $PWD/migrations:/migrations`); the CLI
# reads this path by default, override with `--migrations-dir` if needed.
ENV MIGRATIONS_DIR=/migrations
WORKDIR /migrations

ENTRYPOINT ["clickhouse-migrations"]
