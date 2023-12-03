import os


# Why variant  MINIO_URL_S3 does not work? (it takes from compose var MINIO_URL: s3:9000)
MINIO_URL_S3 = os.environ.get('MINIO_URL')
MINIO_URL_SPARK = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = os.environ.get('MINIO_ROOT_USER') or "minio_access_key"
MINIO_SECRET_KEY = os.environ.get('MINIO_ROOT_PASSWORD') or "minio_secret_key"
SPARK_MASTER = os.environ.get("SPARK_MASTER") or "local"


# Jars for s3 and db
HADOOP_AWS_JAR = 'hadoop-aws-3.3.2.jar'
AWS_JAVA_BUNDLE = 'aws-java-sdk-bundle-1.12.336.jar'
AWS_JAVA_SDK_CORE_JAR = 'aws-java-sdk-core-1.12.336.jar'
AWS_JAVA_SDK_S3_JAR = 'aws-java-sdk-s3-1.12.336.jar'
AWS_JAVA_SDK_JAR = 'aws-java-sdk-1.12.336.jar'
SQLITE_JAR = 'sqlite-jdbc-3.36.0.3.jar'
POSTGRES_JAR = 'postgresql-42.5.0.jar'
JETS3T_JAR = 'jets3t-0.9.4.jar'

# path for docker and for local
if os.environ.get("SPARK_MASTER"):
    path = "/home/airflow/dags/jars/"
else:
    # chang for your local path
    path = "/home/tkachuk/PycharmProjects/AirFlowProject/jars/"
JARS = ', '.join([f'{path}{jar}' for jar in
                  (
                      HADOOP_AWS_JAR,
                      AWS_JAVA_BUNDLE,
                      POSTGRES_JAR,

                      #  I've tried with listed below files also and diff jars version
                      # AWS_JAVA_SDK_JAR,
                      # AWS_JAVA_SDK_CORE_JAR,
                      # AWS_JAVA_SDK_S3_JAR,
                      # JETS3T_JAR,
                  )])

# s3 buckets names for diff api
BITFINEX_BUCKET = "bitfinex-bucket"
POLONIEX_BUCKET = "poloniex-bucket"
# bucket for converted data
DATA_BUCKET = "data-bucket"

# api url
POLONIEX_URL = "https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC"
BITFINEX_URL = "https://api.bitfinex.com/v1/trades/btcusd?limit_trades=500"

# db constants
# I've tried this
# POSTGRES_URL = "jdbc:postgresql//postgresql:airflow"
# POSTGRES_URL = "jdbc:postgresql//localhost:airflow" also with port
POSTGRES_URL = "jdbc:postgresql:airflow"
# all of them gives diff exception
PROPERTIES = {
    "user": os.environ.get("POSTGRES_USER") or "airflow",
    "password": os.environ.get("POSTGRES_PASSWORD") or "airflow",
    "driver": "org.postgresql.Driver",
}
# also try "airflow.statistic"
POSTGRES_TABLE = "statistic"
