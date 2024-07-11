FROM apache/airflow
RUN pip install --upgrade pip
COPY requirements.txt .
COPY ./dags/ ./dags/
RUN pip install -r requirements.txt
USER root
RUN apt-get update
RUN apt-get install wget