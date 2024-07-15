FROM apache/airflow
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
COPY ./dags/ /opt/airflow/dags/
USER root
RUN apt-get update && \
    apt-get install -y wget git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow
