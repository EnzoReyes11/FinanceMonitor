# Worklog

### 20/10/25
- Added terraform scripts to generate BQ dataset
- A script to populate dim_date
- Manually populating DIM Asset now

#### Working On
- Error on AV request
- Read from GCS and write into *`fact_price_history`*


### 19/10/25

#### Done
- Writing to GCS

#### Next Steps

### 18/10/25
Saturday, early morning start. I want to have something more done during the morning, as
I will go climbing in the afternoon! I need to move my body...I've been sitting too
much this past week.

#### Done
- Configured Pylance, now it's able to find packages.
- Some general .vscode configurations.
- Added terraform code to create the GCS bucket
- AVExtract:
  - Connects to get the symbols from the `stocks` table.
  - Queries AV API with the backfill.
  - Tries to write to the GCS bucket.

#### Next steps:
- Actually write to GCS. There were errors. **There is a return on run() finishing the extract job early.**
- Refactor the ENV variables. Rethink what is need. GCP_PROJECT_ID or BQ_PROJECT_ID. 
- Work again in the table schemas. There were some schemas created some time ago, but where not used. 
Look on the `investmentsmonitor` dataset and migrate/bq_dataset. It might be a good time to introduce
the proper schemas with transactions and facts.
- Read from the GCS bucket and load into BQ.
- Generate BQ assets with terraform.



### 17/10/25
Today was very productive. Lots of changes introduced. 
I switched to Claude Sonnet 4.5. It really was very helpfull, 
more than Gemini. 

#### Done
- The monorepo a proper structure now. I'm using UV Workspaces to work
with packages. 
- Building: cloudbuild.yml. That file will be added alongside the Dockerfile on
each standalone job/service. The deployment gets done from the root.
- Makefile was also introduced. It's able to build and update my project infrastructure,
running cloud build and terraform.
- Tests: they are run with pytest from the root directory. Modules can be tested by themself
or everything together.
- AV Extractor: started to update this job to extract and load into GCS.
- AV Loader: draft. Reads from GCS and writes into BQ.
- Airflow: draft of the DAG to extract and load.

#### Next steps:
- Work on the AV Extractor. 
  - It needs to read from BQ the list of Symbols to get (market and other data).
  - Query them and write into GCS.
  - Both for backfill and daily.
  - Review the BQ schemas, from the tables we read and the tables we write to.
  - Work in the data loader.


### 14/10/25
Putting this project in order, as I haven't been working on it for a while now.
It doesn't 
help the situation that this is a mono repo, with several different parts of the finance
pipeline. So There are different projects in different states. 

Last thing I was working on was the LECAPs and BONCAPs (Fixed Income, Renta Fija) ingestion
pipeline. 

#### Done:
- Integrated work done in the branch lecap-scraper into main.
- Fixed the .devcontainer settings, now files in the host system are owned by my user instead of root.
- Added uv into .devcontainer features.
- Added the Worklog files. Hopefully this will help with the issue of not knowing what I was doing when
time passes.
- Added some more files into .gitignore.
- **Started the rearchitect branch. See below**

#### Rearchitect branch
Done: 
- Set the file structure.
- Terraform project creation.
- Terraform cloud run job creation.
- Started to work on alphavantage_extractor service.

TODO:
- Deploying the cloud image for alphavantage_extractor:
    > WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager, possibly rendering your system unusable. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv. Use the --root-user-action option if you know what you are doing and want to suppress this warning.
- Migrate the AlphaVantage client. 
  1. It should be separated in two, the client code and the runner. The runner should be on the cloud run job and call the client (a package).
  1. The runner should also write into GCS the output. 
  1. Add the DAG into orchestrator/stocks_dag.py and try it on Cloud Composer.
  1. Call BQ from the runner, to retrieve the list of stocks to use.
  1. Write the ouput from the runner into BQ. Define if the runner will do this, or it will a different step in the DAG.
  1. Write tests for this job.
- Migrate the LECAP' scraper.
- Add transactions into the porfolio. 
  1. Use the new BQ schemas.
  1. Read the transactions log from google sheets.
  1. Create a new LookerStudio dashboard.
- Add/migrate the IOL code.