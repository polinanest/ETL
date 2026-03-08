from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def update_support_dashboard(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run("REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.support_dashboard;")

dag = DAG(
    'analytics_marts',
    default_args=default_args,
    schedule_interval='30 * * * *',
    catchup=False,
)

support_task = PythonOperator(
    task_id='update_support_dashboard',
    python_callable=update_support_dashboard,
    dag=dag,
)