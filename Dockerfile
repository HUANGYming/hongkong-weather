FROM ghcr.io/astral-sh/uv:python3.11-bookworm

WORKDIR /app

ARG UV_DEFAULT_INDEX=https://pypi.org/simple
ARG UV_HTTP_TIMEOUT=300

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_SYNC=1 \
    UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX} \
    UV_HTTP_TIMEOUT=${UV_HTTP_TIMEOUT}

COPY pyproject.toml uv.lock ./
RUN UV_NO_SYNC=0 uv sync --locked --no-dev

COPY hko_common.py ./
COPY update_hko_postgres.py ./
COPY update_hko_realtime_postgres.py ./
COPY cleanup_hko_realtime_raw.py ./
COPY .env ./

ENTRYPOINT ["uv", "run", "python"]
