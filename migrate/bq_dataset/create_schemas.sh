#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- 1. Load Environment Variables from .env file ---
if [ -f "../.env" ]; then
  # Use set -a to export all variables defined in the file
  set -a
  source ../.env
  set +a
  echo "✅ Loaded environment variables from .env file."
else
  echo "❌ Error: .env file not found. Please create one with GCP_PROJECT_ID and BQ_DATASET_ID."
  exit 1
fi

# --- 2. Validate that variables are set ---
if [ -z "$GCP_PROJECT_ID" ] || [ -z "$BQ_DATASET_ID" ]; then
  echo "❌ Error: GCP_PROJECT_ID or BQ_DATASET_ID is not set in the .env file."
  exit 1
fi

echo "🚀 Deploying schemas to project '$GCP_PROJECT_ID' and dataset '$BQ_DATASET_ID'..."

# --- 3. Loop through schemas, substitute placeholders, and apply ---
for f in schemas/*.sql
do
  echo "   - Applying schema from $f..."
  
  # Use sed to replace the placeholder with the actual dataset ID from the .env file,
  # then pipe the resulting SQL to the bq query command.
  sed "s/{{DATASET_ID}}/${BQ_DATASET_ID}/g" "$f" | bq query --project_id=$GCP_PROJECT_ID --use_legacy_sql=false
done

echo "🎉 Schema deployment complete."