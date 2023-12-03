FROM apache/airflow:2.4.1-python3.8

ENV AIRFLOW_HOME=/home/airflow

COPY README.md ${AIRFLOW_HOME}/README.md
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY dags ${AIRFLOW_HOME}/dags
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY jars/ ${AIRFLOW_HOME}/dags/jars
COPY .env ${AIRFLOW_HOME}/.env
#COPY init.sql /docker-entrypoint-initdb.d/init.sql

USER root

RUN apt-get update && apt-get install wget -y
RUN wget https://github.com/AdoptOpenJDK/openjdk8-upstream-binaries/releases/download/jdk8u252-b09/OpenJDK8U-jdk_x64_linux_8u252b09.tar.gz -O /tmp/jdk8.tar.gz
RUN mkdir /usr/lib/jdk && tar xfv /tmp/jdk8.tar.gz -C /usr/lib/jdk && rm /tmp/jdk8.tar.gz

ENV JAVA_HOME=/usr/lib/jdk/openjdk-8u252-b09
ENV PATH="/usr/lib/jdk/openjdk-8u252-b09/bin:${PATH}"

USER airflow

RUN pip install --upgrade pip\
    && pip install celery \
    && pip install plyvel==1.3.0 \
    && pip install -r ${AIRFLOW_HOME}/requirements.txt

EXPOSE 8080 8000 5555

ENTRYPOINT [ "sh", "/home/airflow/entrypoint.sh" ]