CREATE TABLE IF NOT EXISTS `{{DATASET_ID}}.dim_date` (
  -- Surrogate key in YYYYMMDD format for easy joins and readability.
  date_key INTEGER NOT NULL,
  -- The actual date, useful for native date functions.
  full_date DATE NOT NULL,
  
  -- Standard date components for easy grouping.
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  month_name STRING,
  day INTEGER,
  day_of_week_name STRING,
  is_weekend BOOLEAN,
  
  -- RECOMMENDED HOLIDAY IMPLEMENTATION:
  -- An array containing the codes of markets on holiday for this date.
  -- Example: ['USA', 'ARG'] if both are on holiday. Empty if none are.
  holiday_markets ARRAY<STRING>
)
OPTIONS (
  description = 'A dimension table containing a row for each date, with scalable holiday tracking.'
);