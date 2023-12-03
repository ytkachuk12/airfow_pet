# import requests
#
# from datetime import datetime, timedelta
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
#
# from minio_client import MinioClient
#
# # BITFINEX = 'https://api.bitfinex.com/v1/trades/btcusd?limit_trades=500'
#
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime.now(),
#     "email": ["ytkachuk12@gmail.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1)
# }
#
#
# # @dag(
# #     schedule=None,
# #     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
# #     catchup=False,
# #     tags=['example'],
# # )
#
#
# def print_hi():
#     print('Hi!!!!!!!!!!!!!!!!!')
#
#
# def pipeline(**kwargs):
#
#     ti = kwargs['ti']
#     ti.xcom_push("start message", "Start")
#
#     minio_client = MinioClient()
#     minio_client.get_or_create_minio_clent()
#
#     message = minio_client.get_or_create_bucket()
#     ti.xcom_push("creation bucket message:", message)
#
#     message = minio_client.put_data_into_bucket(
#         minio_client.bitfinex_name, minio_client.bitfinex_url
#     )
#     ti.xcom_push("put data into bitfinex bucket message", message)
#
#     message = minio_client.put_data_into_bucket(
#         minio_client.poloniex_name, minio_client.poloniex_url
#     )
#     ti.xcom_push("put data into poloniex bucket message", message)
#
#
# with DAG("test_dag", default_args=default_args, schedule=timedelta(minutes=1)) as dag:
#     dag.doc_md = __doc__
#
#     task1 = PythonOperator(
#         task_id="print_hi", python_callable=print_hi
#     )
#
#     task2 = PythonOperator(
#         task_id="minio", python_callable=pipeline
#     )
#     task1 >> task2
