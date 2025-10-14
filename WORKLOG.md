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

#### Next steps:
- Remove unused code
- Rename the sub projects, they should make sense
- Add README.md and PROJECT.md
- Terraform use should be clear.
- Define if I will migrate the pipelines to Airflow 3. If so, what can be reused.
- Define if migrating Flask code to FastAPI.