resource "google_bigquery_dataset" "stocks" {
  project    = var.project_id
  dataset_id = var.bq_dataset_id
  location   = var.region

  description = "Stock market data and portfolio analytics"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# dim_date - Date dimension with trading calendar
resource "google_bigquery_table" "dim_date" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stocks.dataset_id
  table_id   = "dim_date"

  description = "Date dimension with per-market trading status"

  # Range partitioning by date_key
  range_partitioning {
    field = "date_key"
    range {
      start    = 20000101
      end      = 20501231
      interval = 10000
    }
  }

  schema = jsonencode([
    {
      name        = "date_key"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Date in YYYYMMDD format"
    },
    {
      name        = "full_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Actual date value"
    },
    {
      name        = "year"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Year"
    },
    {
      name        = "quarter"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Quarter (1-4)"
    },
    {
      name        = "month"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Month (1-12)"
    },
    {
      name        = "month_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Month name"
    },
    {
      name        = "day"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Day of month"
    },
    {
      name        = "day_of_week"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Day of week (1=Monday, 7=Sunday)"
    },
    {
      name        = "day_of_week_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Day name"
    },
    {
      name        = "markets_open"
      type        = "STRING"
      mode        = "REPEATED"
      description = "Array of market codes that are open (e.g., ['US', 'UK'])"
    },
    {
      name        = "markets_closed"
      type        = "STRING"
      mode        = "REPEATED"
      description = "Array of market codes that are closed"
    },
    {
      name = "holidays"
      type = "RECORD"
      mode = "REPEATED"
      description = "Holiday information for closed markets"
      fields = [
        {
          name        = "market"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Market code"
        },
        {
          name        = "name"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Holiday name"
        }
      ]
    }
  ])
}

# dim_asset - Asset dimension
resource "google_bigquery_table" "dim_asset" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stocks.dataset_id
  table_id   = "dim_asset"

  description = "Dimension table for investment assets (stocks, bonds, currencies, etc.)"

  schema = jsonencode([
    {
      name        = "asset_key"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Unique identifier for the asset"
    },
    {
      name        = "ticker_symbol"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Ticker symbol"
    },
    {
      name        = "asset_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Full name of the asset"
    },
    {
      name        = "asset_type"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Type: STOCK, BOND, ETF, CRYPTO, CURRENCY, COMMODITY"
    },
    {
      name        = "asset_subtype"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Subtype: COMMON_STOCK, GOVERNMENT_BOND, etc."
    },
    {
      name        = "exchange_code"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Exchange: NYSE, NASDAQ, LSE, BCBA, etc."
    },
    {
      name        = "exchange_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Exchange name: New York Stock Exchange, Bolsa y Mercados de Buenos Aires."
    },
    {
      name        = "exchange_country"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Market country code (ISO 3166-1 alpha-2)"
    },
    {
      name        = "currency_code"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Currency code (ISO 4217)"
    },
    {
      name        = "isin"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "International Securities Identification Number"
    },
    {
      name        = "is_active"
      type        = "BOOLEAN"
      mode        = "NULLABLE"
      description = "Whether the asset is actively traded"
    },
    {
      name        = "metadata"
      type        = "JSON"
      mode        = "NULLABLE"
      description = "Additional asset-specific metadata"
    },
    {
      name        = "last_updated"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Last update timestamp"
    }
  ])
}

# fact_price_history - Historical price data
resource "google_bigquery_table" "fact_price_history" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stocks.dataset_id
  table_id   = "fact_price_history"

  description = "Historical OHLCV price data in local currency"

  time_partitioning {
    type  = "DAY"
    field = "snapshot_date"
  }

  clustering = ["asset_key"]

  schema = jsonencode([
    {
      name        = "snapshot_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Exact timestamp of the price snapshot"
    },
    {
      name        = "snapshot_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Date of the snapshot (for partitioning)"
    },
    {
      name        = "asset_key"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Foreign key to dim_asset"
    },
    {
      name        = "open_price"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Opening price"
    },
    {
      name        = "high_price"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Highest price"
    },
    {
      name        = "low_price"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Lowest price"
    },
    {
      name        = "close_price"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Closing price"
    },
    {
      name        = "volume"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Trading volume"
    },
    {
      name        = "price_currency"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Currency of the prices (ISO 4217)"
    },
    {
      name        = "data_source"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Data source: ALPHAVANTAGE, YAHOO, IOL, etc."
    },
    {
      name        = "is_adjusted"
      type        = "BOOLEAN"
      mode        = "NULLABLE"
      description = "Whether prices are adjusted for splits/dividends"
    },
    {
      name        = "ingestion_timestamp"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "When the data was ingested"
    }
  ])
}

# transactions - User transactions
resource "google_bigquery_table" "transactions" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stocks.dataset_id
  table_id   = "transactions"

  description = "User transactions (buys, sells, dividends, splits)"

  time_partitioning {
    type  = "DAY"
    field = "transaction_date"
  }

  clustering = ["user_id", "asset_key"]

  schema = jsonencode([
    {
      name        = "transaction_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique transaction identifier"
    },
    {
      name        = "user_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "User identifier"
    },
    {
      name        = "asset_key"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Foreign key to dim_asset"
    },
    {
      name        = "transaction_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Exact timestamp of transaction"
    },
    {
      name        = "transaction_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Date of transaction (for partitioning)"
    },
    {
      name        = "transaction_type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Type: BUY, SELL, DIVIDEND, SPLIT"
    },
    {
      name        = "quantity"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Number of units (always positive)"
    },
    {
      name        = "price_per_unit"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Price per unit in transaction currency"
    },
    {
      name        = "commission"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Commission or fees paid"
    },
    {
      name        = "currency_code"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Currency of the transaction (ISO 4217)"
    },
    {
      name        = "notes"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Optional notes"
    },
    {
      name        = "source"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Source: MANUAL, IMPORT, API"
    },
    {
      name        = "created_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "When the transaction was created"
    },
    {
      name        = "synced_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "When synced from Postgres (if applicable)"
    }
  ])
}

# fact_portfolio_snapshot - Portfolio snapshots
resource "google_bigquery_table" "fact_portfolio_snapshot" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stocks.dataset_id
  table_id   = "fact_portfolio_snapshot"

  description = "Daily portfolio holdings calculated from transactions and prices"

  time_partitioning {
    type  = "DAY"
    field = "snapshot_date"
  }

  clustering = ["user_id", "asset_key"]

  schema = jsonencode([
    {
      name        = "snapshot_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Date of the snapshot"
    },
    {
      name        = "user_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "User identifier"
    },
    {
      name        = "asset_key"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Foreign key to dim_asset"
    },
    {
      name        = "quantity_held"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Number of units held"
    },
    {
      name        = "avg_cost_basis"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Weighted average purchase price"
    },
    {
      name        = "total_cost_basis"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Total amount invested (in asset currency)"
    },
    {
      name        = "cost_basis_currency"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Currency of cost basis"
    },
    {
      name        = "current_price"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Current price per unit"
    },
    {
      name        = "price_currency"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Currency of current price"
    },
    {
      name        = "market_value"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Current market value (quantity * current_price)"
    },
    {
      name        = "unrealized_gain_loss"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Unrealized gain/loss (market_value - total_cost_basis)"
    },
    {
      name        = "unrealized_gain_loss_pct"
      type        = "NUMERIC"
      mode        = "NULLABLE"
      description = "Unrealized gain/loss percentage"
    },
    {
      name        = "calculated_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "When this snapshot was calculated"
    },
    {
      name        = "data_quality"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Data quality flag: COMPLETE, MISSING_PRICE, STALE_PRICE"
    }
  ])
}

# Outputs
output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.stocks.dataset_id
  description = "BigQuery dataset ID"
}

output "bigquery_tables" {
  value = {
    dim_date               = google_bigquery_table.dim_date.table_id
    dim_asset              = google_bigquery_table.dim_asset.table_id
    fact_price_history     = google_bigquery_table.fact_price_history.table_id
    transactions           = google_bigquery_table.transactions.table_id
    fact_portfolio_snapshot = google_bigquery_table.fact_portfolio_snapshot.table_id
  }
  description = "BigQuery table IDs"
}