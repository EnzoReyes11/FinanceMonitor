FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

ENV PYTHONUNBUFFERED=True

WORKDIR /app

# Copy workspace files
COPY pyproject.toml uv.lock ./
COPY packages/alphavantage ./packages/alphavantage
COPY data_pipelines/stocks/services/alphavantage_loader ./data_pipelines/stocks/services/alphavantage_loader

RUN uv sync --frozen --no-dev

WORKDIR /app/data_pipelines/stocks/services/alphavantage_loader

CMD ["uv", "run", "load"]


