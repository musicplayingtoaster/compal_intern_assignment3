FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY backend/pyproject.toml backend/uv.lock ./

COPY backend/src/ ./src/
RUN uv sync --frozen --no-install-project

CMD ["uv", "run", "assignment3"]
