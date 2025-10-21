# Install 

## Locally

*Note*: UV required.
```
 $ cd FinanceMonitor/data_pipelines/stocks/services/alphavantage_loader
 $ uv sync
 $ uv run extract
 $ uv run pytest
```

## Locally in a Docker Container

*Note*: Docker required.
```
 $ cd FinanceMonitor 
 $ docker build -f data_pipelines/stocks/services/alphavantage_loader/Dockerfile \
   -t alphavantage-loader:<TAG> .
 $ docker run alphavantage-loader:<TAG>
```

## GCP

*Note*: GCP SDK required.

### Deploy
```
 $ cd FinanceMonitor
 $ gcloud config set project <GCP PROJECT ID>
 $ gcloud auth login
 $ make deploy-alphavantage_loader
```


### Deploy and Execute
```
 $ cd financemonitor
 $ gcloud config set project <GCP PROJECT ID>
 $ gcloud auth login
 $ make test-alphavantage_loader
```