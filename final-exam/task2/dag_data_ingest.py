import datetime
from airflow import DAG
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

S3_BUCKET       = "etl002"
SERVICE_ACCOUNT = "ajed9tgtqeehe5ednct0"
SUBNET_ID       = "e9bt8ia0gdnnqrq1o77f"
PYSPARK_JOB     = f"s3a://{S3_BUCKET}/scripts/pyspark_job.py"
SSH_PUBLIC_KEY  = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHoohC3H9xCtIAhtPWISFdjBqLflT0+6aC/9yCjq8rXN polinanest@github"

with DAG(
    dag_id="DATA_INGEST",
    schedule_interval="@daily",
    start_date=datetime.datetime(2026, 6, 1),
    catchup=False,
    tags=["etl", "dataproc"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name="etl-cluster",
        zone="ru-central1-a",
        subnet_id=SUBNET_ID,
        service_account_id=SERVICE_ACCOUNT,
        s3_bucket=S3_BUCKET,
        cluster_image_version="2.1",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-hdd",
        masternode_disk_size=40,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-hdd",
        datanode_disk_size=128,
        datanode_count=1,
        services=["HDFS", "YARN", "SPARK"],
        ssh_public_keys=SSH_PUBLIC_KEY,
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark",
        cluster_id="{{ task_instance.xcom_pull('create_cluster', key='cluster_id') }}",
        main_python_file_uri=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_id="{{ task_instance.xcom_pull('create_cluster', key='cluster_id') }}",
        trigger_rule="all_done",
    )

    create_cluster >> run_pyspark >> delete_cluster
