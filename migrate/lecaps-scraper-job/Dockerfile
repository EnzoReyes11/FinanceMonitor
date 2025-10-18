#FROM python:3.10-slim
FROM ghcr.io/astral-sh/uv:0.8.14-python3.10-bookworm-slim

WORKDIR /app

COPY pyproject.toml .
COPY main.py .
COPY etl/ ./etl/
COPY uv.lock .

RUN  uv sync \
        --locked \
        --no-dev \
        --no-install-project

# Make port 8080 available to the world outside this container
# Google Cloud Run will set this env var automatically
ENV PORT=8080
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN useradd -m appuser
USER appuser


CMD ["uv", "run", "main.py"]
