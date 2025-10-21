# Finance Monitor

This is part of my suit of Financial Investments tools. 

In this monorepo, everything related to ETL of the required data is handled.

- Portfolio status from a spreadsheet with my transactions.
- Stock, bonds, ETF, etc daily and backfill ingestions.
  - Alpha Vantage for US market
  - IOL for Argentina.
- Argentina fixed rate income from IAMC.
- ARS / USD from BCRA.

The project is run on Google Cloud Platform.

## Design
```
financemonitor/
├── .github/
│   └── workflows/
│       └── ci.yml
├── data_pipelines/
│   ├── stocks/
│   │   ├── orchestrator/
│   │   │   └── stocks_dag.py
│   │   ├── services/
│   │   │   ├── alphavantage_extractor/    # Service 1: API -> GCS
│   │   │   │   ├── main.py
│   │   │   │   ├── Dockerfile
│   │   │   │   └── tests/
│   │   │   │       └── test_main.py
│   │   │   ├── iol_extractor/             # Service 2: API -> GCS
│   │   │   │   ├── main.py
│   │   │   │   ├── Dockerfile
│   │   │   │   └── tests/
│   │   │   │       └── test_main.py
│   │   │   └── stock_loader/              # Service 3: GCS -> BQ
│   │   │       ├── main.py
│   │   │       ├── Dockerfile
│   │   │       └── tests/
│   │   │           └── test_main.py
│   │   └── bq_schemas/
│   │       └── stocks.json
│   ├── bonds/
│   │   ├── orchestrator/
│   │   │   └── bonds_dag.py
│   │   ├── services/
│   │   │   ├── lecaps_extractor/          # Service 1: Scrape -> GCS
│   │   │   │   ├── main.py
│   │   │   │   ├── Dockerfile
│   │   │   │   └── tests/
│   │   │   │       └── test_main.py
│   │   │   └── bond_loader/               # Service 2: GCS -> BQ
│   │   │       ├── main.py
│   │   │       ├── Dockerfile
│   │   │       └── tests/
│   │   │           └── test_main.py
│   │   └── bq_schemas/
│   │       └── lecaps.json
│   └── ... (and so on for other data domains)
├── packages/                              # Shared, reusable code
│   └── financemonitor_helpers/
│       ├── __init__.py
│       ├── bigquery_client.py           # BQ helper functions
│       └── gcs_client.py                # GCS helper functions (new!)
└── terraform/                  
    ├── gcp_project/
    │   ├── main.tf
    │   └── variables.tf
    └── services/
        ├── cloud_run.tf
        └── bq.tf
        ├── main.tf
        └── modules/
            └── main.tf
```

## Backlog
- Migrate the AlphaVantage client. 
  1. ~~It should be separated in two, the client code and the runner. The runner should be on the cloud run job and call the client (a package)~~.
  1. ~~The runner should also write into GCS the output. ~~
  1. ~~Call BQ from the runner, to retrieve the list of stocks to use.~~
  1. Read from GCS and write into BQ.
  1. Write tests for this job.
  1. Add the DAG into orchestrator/stocks_dag.py and try it on Cloud Composer.
- Migrate the LECAP' scraper.
- Add transactions into the porfolio. 
  1. Use the new BQ schemas.
  1. Read the transactions log from google sheets.
  1. Populate assets not in dim_asset using the transaction's data.
  1. Create a new LookerStudio dashboard.
- Add/migrate the IOL code.
- Expose endpoints for the Portfolio API to query on demand values. This could be a Cloud Function.
- Consider integrating yfinance to work aroung the AV daily limit.

## Technologies
- Google Cloud Platform (GCP)
- Python
- Flask
- Cloud Run
- Terraform
- Cloud Workflows
- Bigquery
- Cloud Storage
- Airflow 3