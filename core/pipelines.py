from core.minio_client import MinioClient
from core.spark import Spark


def start():
    print("START")


def get_or_create_buckets(ti):
    """Create or return existing S3 buckets"""

    minio_client = MinioClient()
    minio_client.get_or_create_minio_client()

    message = minio_client.get_or_create_bucket(minio_client.bitfinex_bucket)
    ti.xcom_push(key="creation bitfinex bucket message:", value=message)

    message = minio_client.get_or_create_bucket(minio_client.poloniex_bucket)
    ti.xcom_push(key="creation poloniex bucket message:", value=message)

    message = minio_client.get_or_create_bucket(minio_client.data_bucket)
    ti.xcom_push(key="creation data bucket message:", value=message)


def put_data_bitfinex(ti):
    """Get data from bitfinex api and save it into S3 bucket"""

    minio_client = MinioClient()
    minio_client.get_or_create_minio_client()
    message = minio_client.put_data_into_bucket(
        minio_client.bitfinex_bucket, minio_client.bitfinex_url
    )

    ti.xcom_push(key=f"put data into {minio_client.bitfinex_bucket} message", value=message)


def put_data_poloniex(ti):
    """Get data from poloniex api and save it into S3 bucket"""

    minio_client = MinioClient()
    minio_client.get_or_create_minio_client()
    message = minio_client.put_data_into_bucket(
        minio_client.poloniex_bucket, minio_client.poloniex_url
    )

    ti.xcom_push(key=f"put data into {minio_client.poloniex_bucket} bucket message", value=message)


def get_convert_data(ti):
    """Create spark session. Get data from s3 using spark and covert it"""

    minio_client = MinioClient()
    minio_client.get_or_create_minio_client()

    spark = Spark()
    spark.session = "airflow_task"

    spark.convert_bitfinex_df()
    spark.convert_poloniex_df()
    spark.get_union_df()

    minio_client.delete_objects(minio_client.bitfinex_bucket)
    minio_client.delete_objects(minio_client.poloniex_bucket)
