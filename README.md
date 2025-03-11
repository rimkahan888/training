# training
## step :
1. create virtual environment by command: `python -m venv venv`
2. activate virtual environment by command: `.\venv\Scripts\activate` or `source venv/bin/activate` if in linux kernel
3. install required packages by command: `pip install -r requirements.txt` 
4. run init process by command: `dlt init github duckdb `
5. make sure you install the extra for duckdb to it To install with pip: `pip install 'dlt[duckdb]>=0.5.1'`
6. In .dlt/secrets.toml, you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.
7. run the pipeline by command: `python github_pipeline.py`
8. Once the pipeline has finished running, you can verify that everything loaded correctly using the following commands:

## Available Pipelines and Verification Commands

1. For DuckDB repository issues and reactions:
   ```bash
   dlt pipeline github_reactions show
   ```

2. For Apache Airflow events:
   ```bash
   dlt pipeline github_events show
   ```

3. For DLThub DLT repository data:
   ```bash
   dlt pipeline github_reactions show
   ```

4. For DLThub DLT stargazers:
   ```bash
   dlt pipeline github_stargazers show
   ```

Each pipeline loads different data into separate datasets in DuckDB:
- `duckdb_issues`: Contains issues and their reactions from the DuckDB repository
- `airflow_events`: Contains events from the Apache Airflow repository
- `dlthub_reactions`: Contains all issues, pull requests, and comments from the DLThub DLT repository
- `dlthub_stargazers`: Contains stargazer information for the DLThub DLT repository
