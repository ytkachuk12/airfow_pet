"""Create Minio client, get or create minio buckets"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from core.pipelines import get_or_create_buckets


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": ["ytkachuk12@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


with DAG("minio_dag", default_args=default_args, schedule="@once") as dag:
    dag.doc_md = __doc__

    task1 = PythonOperator(
        task_id="get_or_create_bucket", python_callable=get_or_create_buckets
    )

    task1
