CREATE TABLE IF NOT EXISTS `{{DATASET_ID}}.fact_investment_snapshot` (
  -- Event Time: The timestamp from the data source (e.g., market close time).
  snapshot_timestamp TIMESTAMP NOT NULL,
  -- Date of the snapshot, derived from snapshot_timestamp. Used for partitioning.
  snapshot_date DATE NOT NULL,
  -- Foreign key to dim_asset. Used for clustering.
  asset_key INTEGER NOT NULL,
  -- The quantity of the asset held at the time of the snapshot.
  quantity NUMERIC,
  -- The price of a single unit in its local currency.
  price_per_unit_local NUMERIC,
  -- Pre-calculated value in local currency (quantity * price_per_unit_local).
  total_value_local NUMERIC,
  -- The exchange rate to a common currency (e.g., ARS to USD) on the snapshot_date.
  exchange_rate_to_usd NUMERIC,
  -- The normalized value in the common currency (total_value_local * exchange_rate_to_usd).
  total_value_usd NUMERIC,
  -- Processing Time: When your pipeline wrote this row into BigQuery. For auditing.
  ingestion_timestamp TIMESTAMP
)
PARTITION BY
  -- Physically segments the table by day for cost and performance gains.
  snapshot_date
CLUSTER BY
  -- Sorts data within each partition by asset_key for faster filtering.
  asset_key
OPTIONS (
  description = 'Fact table storing daily snapshots of each asset held. Grain is one row per asset per day.'
);