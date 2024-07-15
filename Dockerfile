FROM apache/airflow
RUN apt update && apt install git -y
RUN pip install --upgrade pip
COPY requirements.txt .
COPY ./dags/ /opt/airflow/dags/
RUN pip install -r requirements.txt
USER root
RUN apt-get update
RUN apt-get install wget