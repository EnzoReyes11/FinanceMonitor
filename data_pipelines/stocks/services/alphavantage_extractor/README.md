# Install 

## Locally

*Note*: UV required.
```
 $ cd FinanceMonitor/data_pipelines/stocks/services/alphavantage_extractor
 $ uv sync
 $ uv run alphavantage-extract
 $ uv run pytest
```

## Locally in a Docker Container

*Note*: Docker required.
```
 $ cd FinanceMonitor 
 $ docker build -f data_pipelines/stocks/services/alphavantage_extractor/Dockerfile \
   -t alphavantage-extractor:<TAG> .
 $ docker run alphavantage-extractor:<TAG>
```

## GCP

*Note*: GCP SDK required.

### Deploy
```
 $ cd FinanceMonitor
 $ gcloud config set project <GCP PROJECT ID>
 $ gcloud auth login
 $ make deploy-alphavantage_extractor
```


### Deploy and Execute
```
 $ cd financemonitor
 $ gcloud config set project <GCP PROJECT ID>
 $ gcloud auth login
 $ make test-alphavantage_extractor
```