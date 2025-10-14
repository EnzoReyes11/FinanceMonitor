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

## Backlog
- Create the portfolio ingestion. A spreadsheet with transactions needs to be
added into BQ. 
- Read this transactions data to retrieve the latest values.

## Technologies
- Google Cloud Platform (GPC)
- Python
- Flask
- Cloud Run
- Terraform
- Cloud Workflows
- Bigquery