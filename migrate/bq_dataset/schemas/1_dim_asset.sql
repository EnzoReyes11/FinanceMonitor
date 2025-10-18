-- The dataset placeholder will be replaced by the script.
CREATE TABLE IF NOT EXISTS `{{DATASET_ID}}.dim_asset` (
  asset_key INTEGER NOT NULL,
  ticker_symbol STRING,
  asset_name STRING,
  asset_type STRING,
  market_name STRING,
  market_country STRING,
  currency_code STRING,
  is_active BOOLEAN
)
OPTIONS (
  description = 'Dimension table for investment assets.'
);