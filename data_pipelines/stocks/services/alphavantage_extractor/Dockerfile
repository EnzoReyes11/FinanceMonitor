FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ENV PYTHONUNBUFFERED=True

WORKDIR /app

# Copy workspace files
COPY pyproject.toml uv.lock ./
COPY packages/alphavantage ./packages/alphavantage
COPY data_pipelines/stocks/services/alphavantage_extractor ./data_pipelines/stocks/services/alphavantage_extractor

RUN uv sync --frozen --no-dev

WORKDIR /app/data_pipelines/stocks/services/alphavantage_extractor

CMD ["uv", "run", "alphavantage-extract"]


