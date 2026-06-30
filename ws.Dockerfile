FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

COPY backend/pyproject.toml backend/uv.lock ./

COPY /backend/src/ ./src/

RUN uv sync --frozen --no-editable

CMD [".venv/bin/python", "-m", "src.broadcast.broadcast"]

