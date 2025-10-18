# dags/alphavantage_daily.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'alphavantage_daily',
    default_args=default_args,
    description='Daily Alpha Vantage stock data pipeline',
    schedule='0 18 * * 1-5',  # 6 PM on weekdays (after market close)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['stocks', 'alphavantage', 'daily'],
) as dag:
    
    extract = CloudRunExecuteJobOperator(
        task_id='extract_alphavantage',
        project_id='{{ var.value.gcp_project }}',
        region='us-central1',
        job_name='alphavantage-extractor',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'MODE', 'value': 'daily'},
                    {'name': 'RUN_DATE', 'value': '{{ ds }}'},
                ]
            }]
        },
    )
    
    load = CloudRunExecuteJobOperator(
        task_id='load_to_bigquery',
        project_id='{{ var.value.gcp_project }}',
        region='us-central1',
        job_name='alphavantage-loader',
        overrides={
            'container_overrides': [{
                'env': [
                    {'name': 'MODE', 'value': 'daily'},
                    {'name': 'RUN_DATE', 'value': '{{ ds }}'},
                ]
            }]
        },
    )
    
    extract >> load  # Dependency: load runs after extract