FROM python:3.13.16-alpine3.24 as builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENV UV_NO_PROGRESS=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON=3.13

RUN apk update && apk upgrade \ 
    && apk add --no-cache sqlite

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --group server --locked --no-install-project --no-editable

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --group server --locked --no-editable


# !!!! RUNTIME !!!!
FROM python:3.13.16-alpine3.24
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/weetags /app/weetags

WORKDIR /app
ENTRYPOINT ["/app/.venv/bin/sanic", "weetags.server.asgi:app", "--host=0.0.0.0", "--port=8000"]