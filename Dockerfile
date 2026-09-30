# syntax=docker/dockerfile:1

# One image, two processes: the API (default command) and the ticker (`agent-runs-ticker`).
# trellis-contracts is a path dependency on ../agent-contracts; the build receives that
# checkout as the named context `contracts` (docker-compose.yml, `make image`) and places it
# at /agent-contracts, next to /app, so the lockfile's relative path resolves unchanged.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:0.12.14 /uv /usr/local/bin/uv
COPY --from=contracts pyproject.toml README.md /agent-contracts/
COPY --from=contracts src /agent-contracts/src

WORKDIR /app

# Dependencies first, in their own layer: code changes far more often than the lockfile.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
# The migrations ship in the image: the `migrate` service runs them, and both processes
# refuse to start against a schema other than the head revision.
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# /data/blobs: the filesystem blob store's volume, owned by the app user
RUN useradd --create-home --uid 10001 app && mkdir -p /data/blobs \
    && chown -R app:app /app /data/blobs
USER app

EXPOSE 8090

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD python -c "import os,sys,urllib.request; \
port=os.environ.get('RUNS__SERVICE__PORT','8090'); \
sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{port}/health/live', timeout=2).status==200 else 1)"

CMD ["agent-runs"]
