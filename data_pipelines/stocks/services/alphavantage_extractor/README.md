# Install 

## Locally

```
 $ uv venv
 $ uv pip install -r requirements.txt
 $ uv run fastapi dev
```

## GCP

*Note*: GCP SDK required.

```
 $ gcloud config set project <GCP PROJECT ID>
 $ gcloud auth login
 $ gcloud run deploy --source .  alphavantage/extractor --region us-central1
 $ gcloud builds submit --tag us-central1-docker.pkg.dev/enzoreyes-financemonitor-dev/financemonitor/alphavantage-extractor:latest .
```