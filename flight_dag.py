import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID   = os.getenv("PROJECT_ID", "your-gcp-project-id")
REGION       = os.getenv("DATAPROC_REGION", "asia-south1")
ZONE         = os.getenv("DATAPROC_ZONE", "asia-south1-a")
CLUSTER_NAME = os.getenv("DATAPROC_CLUSTER", "flight-cluster-airflow")
GCS_BUCKET   = os.getenv("GCS_BUCKET", "your-spark-bucket")

default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {
            "boot_disk_size_gb": 30
        }
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {
            "boot_disk_size_gb": 30
        }
    },
    "gce_cluster_config": {
        "zone_uri": ZONE,
    },
    "software_config": {
        "image_version": "2.1-debian11"
    }
}

PYSPARK_JOB = {
    "reference":      {"project_id": PROJECT_ID},
    "placement":      {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/spark_batch.py",
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
        ]
    }
}

with DAG(
    dag_id="flight_delay_pipeline",
    default_args=default_args,
    description="Nightly flight delay stats using PySpark on Dataproc",
    schedule_interval="0 1 * * *",   # runs every night at 1 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["flight", "dataproc", "pyspark"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    run_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",  # delete even if job fails
    )

    create_cluster >> run_pyspark >> delete_cluster
