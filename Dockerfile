FROM ghcr.io/astral-sh/uv:python3.11-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_SYNC=1

COPY pyproject.toml uv.lock ./
RUN UV_NO_SYNC=0 uv sync --locked --no-dev

COPY hko_common.py ./
COPY update_hko_postgres.py ./
COPY update_hko_realtime_postgres.py ./
COPY cleanup_hko_realtime_raw.py ./

ENTRYPOINT ["uv", "run", "python"]
