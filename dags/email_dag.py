import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": "test_mail_ytk@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1)
}


with DAG("email", default_args=default_args, schedule=timedelta(hours=24)) as dag:

    task1 = EmailOperator(
        task_id="email",
        to=os.environ.get("MAIL_TO"),
        subject="Daily statistic",
        html_content=" Mail Test ",
    )
