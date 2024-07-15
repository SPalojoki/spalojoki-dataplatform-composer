FROM apache/airflow
COPY requirements.txt .
COPY dbt-requirements.txt .
RUN pip install --upgrade pip && \
pip install -r requirements.txt
RUN python -m venv dbt_env && source dbt_env/bin/activate && \
    pip install --upgrade pip && pip install --no-cache-dir -r dbt-requirements.txt && deactivate
COPY ./dags/ /opt/airflow/dags/
USER root
RUN apt-get update && \
    apt-get install -y wget git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
