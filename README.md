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


## Requirements:
- GCP SDK
- Python 3.12+
- Terraform
- uv

**Note**: Using VSCode is highly recommended, as the devcontainer has all the requirements.
## Installation:

```
$ git clone <repo>
```

### Create the cloud project
1. Enter into the project setup directory.
    ```
    $ cd terraform/gcp_project
    ```
1. Open an editor and update the project variables.
    ```
    $ nano terraform.tfvars
    ```
    - project_id: the GCP Project ID. It needs to be unique.
    - project_name: the GCP Project Name, it can be whatever you want.      
    - billing_account: the GCP Billing account.
1. Init terraform and review the plan
    ```
    $ terraform init
    $ terraform plan -out=plan
    ```
1. If the plan is correct, apply
    ```
    $ terraform apply "plan"
    ```
1. Review on Google Cloud Console that the project has been created.