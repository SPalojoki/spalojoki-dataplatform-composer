import requests
import json
from datetime import datetime, timezone
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "ingest_weather_readings",
    default_args=default_args,
    description="A DAG to ingest latest weather readings from a self-hosted Netatmo weather station",
    schedule_interval="5-59/10 * * * *",  # Every 10 minutes from 5 to 59: 5, 15, 25, 35, 45, 55
    start_date=days_ago(1),
    catchup=False,
)

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
NETATMO_CLIENT_ID = Variable.get("NETATMO_CLIENT_ID")
NETATMO_CLIENT_SECRET = Variable.get("NETATMO_CLIENT_SECRET")
landing_dataset_name = "analytics__landing"
landing_table_name = "netatmo_weather_readings"


# Function to update the Netatmo API tokens
def update_token():
    NETATMO_REFRESH_TOKEN = Variable.get("NETATMO_REFRESH_TOKEN")

    url = "https://api.netatmo.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": NETATMO_REFRESH_TOKEN,
        "client_id": NETATMO_CLIENT_ID,
        "client_secret": NETATMO_CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        new_access_token = response_json.get("access_token")
        new_refresh_token = response_json.get("refresh_token")

        if new_access_token and new_refresh_token:
            Variable.set("NETATMO_ACCESS_TOKEN", new_access_token)
            Variable.set("NETATMO_REFRESH_TOKEN", new_refresh_token)
            logging.info("Tokens updated successfully.")
        else:
            logging.error("Failed to retrieve tokens from response.")
            raise ValueError("Invalid tokens received.")
    except requests.RequestException as e:
        logging.error(f"Request to Netatmo API failed: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


# Function to extract Netatmo readings
def extract_netatmo_readings():
    NETATMO_ACCESS_TOKEN = Variable.get("NETATMO_ACCESS_TOKEN")
    url = "https://api.netatmo.com/api/getstationsdata"
    headers = {"Authorization": f"Bearer {NETATMO_ACCESS_TOKEN}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["body"]["devices"]


# Function to prepare data for BigQuery
def prepare_data(data):
    sdp_metadata = json.dumps({"loaded_at": datetime.now(timezone.utc).isoformat()})
    for row in data:
        row["dashboard_data"] = json.dumps(row["dashboard_data"])
        row["place"] = json.dumps(row["place"])
        row["modules"] = [json.dumps(module) for module in row["modules"]]
        row["sdp_metadata"] = sdp_metadata
    return data


# Function to load data into BigQuery
def load_data(rows):
    gcloud_conn_hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    credentials = gcloud_conn_hook.get_credentials()
    client = bigquery.Client(credentials=credentials)

    table_id = f"{GCP_PROJECT_ID}.{landing_dataset_name}.{landing_table_name}"
    table = client.get_table(table_id)

    errors = client.insert_rows_json(table, rows)
    if errors:
        raise Exception(f"Failed to insert rows: {errors}")


# Gathering function to extract, prepare and load data
def extract_and_load():
    raw_data = extract_netatmo_readings()
    prepared_data = prepare_data(raw_data)
    load_data(prepared_data)


# Task to create the BigQuery table if it doesn't exist
create_table_task = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    project_id=GCP_PROJECT_ID,
    dataset_id=landing_dataset_name,
    table_id=landing_table_name,
    schema_fields=[
        {"name": "_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date_setup", "type": "INT64", "mode": "NULLABLE"},
        {"name": "last_setup", "type": "INT64", "mode": "NULLABLE"},
        {"name": "type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_status_store", "type": "INT64", "mode": "NULLABLE"},
        {"name": "module_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "firmware", "type": "INT64", "mode": "NULLABLE"},
        {"name": "wifi_status", "type": "INT64", "mode": "NULLABLE"},
        {"name": "reachable", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "co2_calibrating", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "data_type", "type": "STRING", "mode": "REPEATED"},
        {"name": "place", "type": "JSON", "mode": "NULLABLE"},
        {"name": "station_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "home_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "home_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "dashboard_data", "type": "JSON", "mode": "NULLABLE"},
        {"name": "modules", "type": "JSON", "mode": "REPEATED"},
        {"name": "sdp_metadata", "type": "JSON", "mode": "NULLABLE"},
    ],
    dag=dag,
)

# Task to update the Netatmo API tokens
update_token_task = PythonOperator(
    task_id="update_token",
    python_callable=update_token,
    dag=dag,
)

# Task to extract, prepare and load data
extract_and_load_task = PythonOperator(
    task_id="extract_and_load",
    python_callable=extract_and_load,
    dag=dag,
)

# Task dependencies
create_table_task >> update_token_task >> extract_and_load_task
