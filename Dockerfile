# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

ENV UV_NO_DEV=1

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

CMD ["uv", "run", "--no-sync", "uvicorn", "resource_catalogue_fastapi:app", "--host", "0.0.0.0", "--port", "8000"]
