"""Put data from API services to S3minio"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from core.pipelines import start, put_data_bitfinex, put_data_poloniex

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


with DAG("save_raw_data", default_args=default_args, schedule=timedelta(minutes=1)) as dag:
    dag.doc_md = __doc__

    task1 = PythonOperator(
        task_id="start", python_callable=start
    )

    task2 = PythonOperator(
        task_id="put_data_bitfinex", python_callable=put_data_bitfinex
    )

    task3 = PythonOperator(
        task_id="put_data_poloniex", python_callable=put_data_poloniex
    )

    task1 >> [task2, task3]
