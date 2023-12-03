from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from core.pipelines import start, get_convert_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": "ytkachuk12@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1)
}


with DAG("aggregation_data_dag", default_args=default_args, schedule=timedelta(hours=1)) as dag:
    dag.doc_md = __doc__

    task1 = PythonOperator(
        task_id="start", python_callable=start
    )

    task2 = PythonOperator(
        task_id="get_convert_data", python_callable=get_convert_data
    )

    # task3 = PythonOperator(
    #     task_id="get_convert_data_poloniex", python_callable=get_convert_data_poloniex
    # )
    #
    # task4 = PythonOperator(
    #     task_id="union_df", python_callable=union_df
    # )
    task1 >> task2
