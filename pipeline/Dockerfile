FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

WORKDIR /code


COPY pipeline.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]