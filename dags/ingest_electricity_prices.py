import os
import requests
import json
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from google.cloud import bigquery

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "ingest_electricity_prices",
    default_args=default_args,
    description="A DAG to ingest latest electricity prices from a public API and store in BigQuery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)

DBT_PROJECT_GITHUB_URL = Variable.get("DBT_PROJECT_GITHUB_URL")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
SAK_PATH = Variable.get("SAK_PATH")
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

landing_dataset_name = "analytics__landing"
landing_table_name = "electricity_prices"



def extract_and_load():
    def extract_electricity_prices():
        url = "https://api.porssisahko.net/v1/latest-prices.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["prices"]

    def prepare_data(data):
        rows_to_load = [
            {
                "start_date": row["startDate"],
                "end_date": row["endDate"],
                "price": row["price"],
                "sdp_metadata": json.dumps({"loaded_at": datetime.now(timezone.utc).isoformat()})
            }
            for row in data
        ]

        return rows_to_load

    def load_data(rows):
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{landing_dataset_name}.{landing_table_name}"
        table = client.get_table(table_id)

        errors = client.insert_rows(table, rows)
        if errors:
            raise Exception(f"Failed to insert rows: {errors}")
        
    raw_data = extract_electricity_prices()
    prepared_data = prepare_data(raw_data)
    load_data(prepared_data)

    

# Create BigQuery table if not exists
create_table = BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    project_id=GCP_PROJECT_ID,
    dataset_id=landing_dataset_name,
    table_id=landing_table_name,
    schema_fields=[
        {"name": "start_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "sdp_metadata", "type": "STRING", "mode": "NULLABLE"}
    ],
)

# Fetch and prepare data
prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=extract_and_load,
    provide_context=True,
)

# Task to run dbt build
dbt_build = BashOperator(
    task_id="dbt_build_electricity_prices",
    bash_command=f"""
        git clone {DBT_PROJECT_GITHUB_URL} {DBT_PROJECT_DIR}
        cd {DBT_PROJECT_DIR}
        source {AIRFLOW_HOME}/dbt_env/bin/activate
        dbt build --profiles-dir ./prod_profile --select stg_electricity_prices+
    """,
    dag=dag,
    env={"GCP_PROJECT_ID": GCP_PROJECT_ID, "SAK_PATH": SAK_PATH},
)

create_table >> prepare_data  >> dbt_build