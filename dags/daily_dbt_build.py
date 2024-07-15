from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "daily_dbt_build",
    default_args=default_args,
    description="A DAG to fetch the latest version of the DBT project from GitHub and run dbt build against BigQuery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)

DBT_PROJECT_GITHUB_URL = Variable.get("DBT_PROJECT_GITHUB_URL")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")


# Task to clone the DBT project from GitHub
clone_or_pull_dbt_project = BashOperator(
    task_id="clone_or_pull_dbt_project",
    bash_command=f"""
        if [ -d "{DBT_PROJECT_DIR}/.git" ]; then
            cd {DBT_PROJECT_DIR} && git pull
        else
            git clone {DBT_PROJECT_GITHUB_URL} {DBT_PROJECT_DIR}
        fi
    """,
    dag=dag,
)

# Debugging task to list the directory contents
list_dbt_project_dir = BashOperator(
    task_id="list_dbt_project_dir",
    bash_command=f"ls -la {DBT_PROJECT_DIR}",
    dag=dag,
)

# Task to run dbt build
dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir ./prod_profile",
    dag=dag,
    env={"GCP_PROJECT_ID": GCP_PROJECT_ID},
)


clone_or_pull_dbt_project >> list_dbt_project_dir >> dbt_build