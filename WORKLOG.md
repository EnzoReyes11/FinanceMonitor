# Worklog

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
- Migrate the LECAP' scraper.
- Add transactions into the porfolio. 
  1. Use the new BQ schemas.
  1. Read the transactions log from google sheets.
  1. Create a new LookerStudio dashboard.
- Add/migrate the IOL code.