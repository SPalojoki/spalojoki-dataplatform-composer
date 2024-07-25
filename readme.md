# SPalojoki Data Platform Composer

This repository contains Airflow DAGs for orchestrating data ingestion and daily production builds on the SPalojoki Data Platform.

## Use Cases and DAGs

Airflow is used for two primary purposes on the SPalojoki Data Platform:

1. Data ingestion
2. Running daily production `dbt build`

### Data Ingestion

Data ingestion DAGs load data from source systems into source tables in the landing dataset, adhering to the ELT process (Extract, Load, Transform). These DAGs **do not modify** the source data. In addition to loading data into the source tables, the DAGs build downstream models for immediate availability.

#### Data Ingestion DAGs

| DAG Name                 | Source System        | Description                                                                                                 |
|--------------------------|----------------------|-------------------------------------------------------------------------------------------------------------|
| Ingest electricity prices| api.porssisahko.net  | Fetches hourly NordPool electricity prices for the next day after they are released.                        |
| Ingest weather readings  | Netatmo Connect API  | Retrieves weather data from an on-site, self-hosted weather station at a summer residence every ten minutes.|

The ingestion jobs require write access to the BigQuery datasets. They use the `google_cloud_platform` connection for authentication with a Google Cloud Service Account. For more details, refer to the *Variables, Secrets, and Connections* section.

### Daily Production `dbt build`

The production `dbt build` runs daily at midnight UTC. Given the relatively low volume of data, all models are refreshed regardless of whether new data has been ingested or the models have been updated. As data volumes grow, consider using incremental build mode.

The DAG uses a service account JSON file (deployed via the Airflow Ansible role) for authentication with BigQuery.

## Variables, Secrets, and Connections

To set up the DAGs, you need to configure variables, secrets, and connections in the Airflow UI when initializing a new Airflow instance.

### Connections

Data ingestion jobs use the `google_cloud_platform` connection module, which is specified in `requirements.txt` for authentication. The connection must be named `google_cloud_default` (set in the `Connection ID` field), and the service account private key JSON file must be added to the `Keyfile JSON` field. The service account key can be retrieved from Terraform outputs and **base64 decoded**.

### Variables and Secrets

The following variables and secrets must be configured for the DAGs to function correctly:

| Variable Name           | Value                                                      |
|-------------------------|------------------------------------------------------------|
| DBT_PROJECT_DIR         | ./spalojoki-dataplatform-dbt                               |
| DBT_PROJECT_GITHUB_URL  | https://github.com/SPalojoki/spalojoki-dataplatform-dbt.git|
| GCP_PROJECT_ID          | wise-key-423412-q8                                         |
| SAK_PATH                | /opt/airflow/dbt_sak.json                                  |
| NETATMO_CLIENT_ID       | *Get from Netatmo Connect*                                 |
| NETATMO_CLIENT_SECRET   | *Get from Netatmo Connect*                                 |
| NETATMO_ACCESS_TOKEN    | *Get from Netatmo Connect*                                 |
| NETATMO_REFRESH_TOKEN   | *Get from Netatmo Connect*                                 |

## Environments

### Production Setup

Despite the repository's name, the Airflow instance is self-hosted, not using Google Cloud Composer.

The DAGs are included in a custom Docker image defined in the `Dockerfile` in this repository. Upon merging to the main branch, GitHub Actions build and push the Docker image to Google Cloud Artifact Registry. Watchtower monitors the registry for changes and pulls the updated image with the updated DAGs to the VM.

The Airflow configuration, including the `docker-compose.yml`, is defined and deployed on a VM using the [SPalojoki Infrastructure *Airflow Ansible role*](https://github.com/SPalojoki/spalojoki-infrastructure/tree/main/ansible).

### Development Setup

A local Airflow development setup can be created by configuring a development `docker-compose` file to spin up a local Airflow instance. However, this development `docker-compose` file has not yet been created, so there is no local development environment available.

As a workaround, you can test changes by building and pushing the local development image to the artifact registry before committing them to version control. This avoids bloating the Git history.

For Apple Silicon, use the `docker buildx` utility:

```sh
docker buildx build --platform linux/amd64 -t europe-west1-docker.pkg.dev/wise-key-423412-q8/spalojoki-artifact-registry/spalojoki-dataplatform-composer --push .
```

For x86-based systems, use the regular `docker build`:

```sh
docker build europe-west1-docker.pkg.dev/wise-key-423412-q8/spalojoki-artifact-registry/spalojoki-dataplatform-composer --push .
```

Use this approach with caution to avoid affecting the production environment, especially when scheduled DAGs are about to execute.

## Why Airflow?

Initially, the SPalojoki Data Platform employed a combination of Google Cloud services for data ingestion and builds. Data ingestion was managed using Google Cloud Functions, Google Cloud Scheduler, and Pub/Sub queues, while DBT builds were executed through Google Cloud Run and Docker containers. This setup was both lightweight and flawless in operation.

However, despite Airflow being somewhat excessive for SPalojoki Data Platform's needs, it was implemented for several key reasons. Primarily, using Airflow offers a valuable learning experience with an industry-standard orchestration tool. Moreover, it consolidates all processes into a single, cohesive platform, reducing the complexity and overhead associated with managing numerous microservices. This approach not only streamlines operations but also provides a more organized and maintainable system.