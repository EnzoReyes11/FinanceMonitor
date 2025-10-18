#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- 1. Load Environment Variables from .env file ---
if [ -f "../.env" ]; then
  set -a
  source "../.env"
  set +a
  echo "✅ Loaded environment variables from .env file."
else
  echo "❌ Error: .env file not found."
  exit 1
fi

# --- 2. Validate that variables are set ---
if [ -z "$GCP_PROJECT_ID" ] || [ -z "$BQ_DATASET_ID" ] || [ -z "$GCS_BUCKET_NAME" ] || [ -z "$GCS_BUCKET_LOCATION" ]; then
  echo "❌ Error: Required variables not set in .env file."
  echo "   Please ensure GCP_PROJECT_ID, BQ_DATASET_ID, GCS_BUCKET_NAME, and GCS_BUCKET_LOCATION are defined."
  exit 1
fi

# --- Define filenames and table names ---
CSV_FILE="dim_date.csv"
STAGING_TABLE="dim_date_staging"
FINAL_TABLE="dim_date"
GCS_BUCKET_URI="gs://${GCS_BUCKET_NAME}"
GCS_FILE_URI="${GCS_BUCKET_URI}/${CSV_FILE}"

echo "🚀 Starting load process for ${FINAL_TABLE}..."

# --- 3. Run the ETL Process ---

# Step A: Ensure GCS Bucket Exists
echo "   - Step 1/5: Checking for GCS bucket ${GCS_BUCKET_NAME}..."
if ! gsutil ls -b "${GCS_BUCKET_URI}" >& /dev/null; then
  echo "     -> Bucket not found. Creating new bucket in ${GCS_BUCKET_LOCATION}..."
  gsutil mb -p "${GCP_PROJECT_ID}" -l "${GCS_BUCKET_LOCATION}" "${GCS_BUCKET_URI}"
  echo "     -> Bucket created successfully."
else
  echo "     -> Bucket already exists."
fi

# Step B: Upload the local CSV to Google Cloud Storage
echo "   - Step 2/5: Uploading ${CSV_FILE} to ${GCS_FILE_URI}..."
gsutil cp "${CSV_FILE}" "${GCS_FILE_URI}"

# Step C: Load data from GCS into a staging BigQuery table
echo "   - Step 3/5: Loading from GCS into staging table ${STAGING_TABLE}..."
bq load \
  --project_id="${GCP_PROJECT_ID}" \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --autodetect \
  "${BQ_DATASET_ID}.${STAGING_TABLE}" \
  "${GCS_FILE_URI}"

# Step D: Execute SQL to transform from staging to the final table
echo "   - Step 4/5: Transforming data into final table ${FINAL_TABLE}..."
SQL_TRANSFORM="
  MERGE \`${GCP_PROJECT_ID}.${BQ_DATASET_ID}.${FINAL_TABLE}\` T
  USING \`${GCP_PROJECT_ID}.${BQ_DATASET_ID}.${STAGING_TABLE}\` S
  ON T.date_key = S.date_key
  WHEN NOT MATCHED THEN
    INSERT (date_key, full_date, year, quarter, month, month_name, day, day_of_week_name, is_weekend, holiday_markets)
    VALUES (date_key, full_date, year, quarter, month, month_name, day, day_of_week_name, is_weekend, IF(holiday_markets = '', [], SPLIT(holiday_markets, '|')))
"
bq query --project_id="${GCP_PROJECT_ID}" --use_legacy_sql=false "${SQL_TRANSFORM}"

# Step E: Clean up staging resources
echo "   - Step 5/5: Cleaning up staging resources..."
bq rm -t -f "${GCP_PROJECT_ID}:${BQ_DATASET_ID}.${STAGING_TABLE}"
gsutil rm "${GCS_FILE_URI}"

echo "🎉 Load process complete for ${FINAL_TABLE}."