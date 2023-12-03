#!/bin/bash

# Initialize database, if not yet
airflow db init

# Create admin user
airflow users create --username="$AIRFLOW_USERNAME" \
                     --firstname="$AIRFLOW_FIRSTNAME" \
                     --lastname="$AIRFLOW_LASTNAME" \
                     --role=Admin \
                     --email="$AIRFLOW_EMAIL"  \
                     --password="$AIRFLOW_PASSWORD"

# Run webserver
airflow webserver -p 8000 &

# Run redis UI
airflow celery flower &

# Run scheduler
airflow scheduler
