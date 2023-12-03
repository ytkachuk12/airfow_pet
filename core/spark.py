"""
Contains SparkSession obj, abstract method for creation DataFrame,
method for writing data frame to csv file
"""
import os
from datetime import datetime
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum, avg, lit, when, from_unixtime, max, min
from pyspark.sql.types import DecimalType
from pyspark.sql.dataframe import DataFrame

from core.schemas import BITFINEX_SCHEMA, POLONIEX_SCHEMA
from core.constants import (
    JARS,
    MINIO_URL_SPARK,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SPARK_MASTER,
    BITFINEX_BUCKET,
    POLONIEX_BUCKET,
    DATA_BUCKET,
    POSTGRES_URL,
    POSTGRES_TABLE,
    PROPERTIES
)


class Spark:
    """Contains SparkSession obj and abstract method for creation DataFrame"""

    bitfinex_path = f"s3a://{BITFINEX_BUCKET}/"
    poloniex_path = f"s3a://{POLONIEX_BUCKET}/"
    data_bucket_path = f"s3a://{DATA_BUCKET}/"

    def __init__(self):
        self._session = None
        self.bitfinex_df = None
        self.poloniex_df = None
        self.union_df = None
        self.statistic_df = None

    @property
    def session(self):
        """Returns SparkSession obj"""
        print("OBJ")
        return self._session

    @session.setter
    def session(self, name: str = "spark_app") -> SparkSession:
        """Set spark obj name and create spark session"""

        print(MINIO_URL_SPARK, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SPARK_MASTER)

        conf = (
            SparkConf()
            .set('spark.jars', JARS)
            .set("spark.hadoop.fs.s3a.endpoint", MINIO_URL_SPARK)
            .set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .set("spark.hadoop.fs.s3a.path.style.access", 'true')
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
        self._session = (
            SparkSession.builder
            .master(SPARK_MASTER)
            .appName(name)
            .config(conf=conf)
            .getOrCreate()
        )

        print('Created SparkContext: ', self._session.sparkContext)
        print(
            'Hadoop version: ',
            self._session.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        )

    def convert_bitfinex_df(self):
        """ Create df from bitfinex-bucket data and convert it"""

        self.bitfinex_df = (
            self._session.read  # The DataFrameReader
            .option('inferSchema', 'true')  # Automatically infer data types & column names
            .schema(BITFINEX_SCHEMA)
            .json(self.bitfinex_path)  # Creates a DataFrame from JSON after reading in the file
            # convert timestamp to date
            .select(
                "exchange",
                from_unixtime(col("timestamp")).alias("date"),
                "type",
                "amount",
                "price"
            )
            .distinct()
        )

        self.bitfinex_df.show()

    def convert_poloniex_df(self):
        """ Create df from poloniex-bucket data and convert it"""

        self.poloniex_df = (
            self._session.read  # The DataFrameReader
            .option('inferSchema', 'true')  # Automatically infer data types & column names
            .schema(POLONIEX_SCHEMA)
            .json(self.poloniex_path)  # Creates a DataFrame from JSON after reading in the file
            .withColumn("exchange", when(col("date").isNotNull(), "poloniex"))
            .select("exchange", "date", "type", "amount", col("rate").alias("price"))
            .distinct()
        )

        self.poloniex_df.show()

    def get_union_df(self):
        """Union data from bitfinex and poloniex api's"""

        self.union_df = (self.bitfinex_df.union(self.poloniex_df))
        self.union_df.show()

    def get_statistic_df(self):
        """Count statistic"""

        self.statistic_df = (
            self.union_df
            .groupBy('exchange', 'type')
            .agg(
                avg("price").alias("avg_price"),
                avg("amount").alias("avg_amount"),
                sum("amount").alias("sum_amount"),
                lit(
                    datetime.today().strftime("%Y-%m-%d %H") + ":00:00"
                ).alias("date")
            )
            .select("exchange", "date", "type", "avg_price", "avg_amount", "sum_amount")
        )
        self.statistic_df.show()

    def save_data_to_bucket(self):
        """Write data to the bucket in the parquet format"""

        # file_name = datetime.today().strftime("%Y-%m-%d %H") + ":00:00.parquet"
        file_name = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        full_path = self.data_bucket_path + file_name
        (
            self.union_df.write  # Our DataFrameWriter
            .option('compression', 'snappy')  # One of none, snappy, gzip, and lzo
            .mode('overwrite')
            .parquet(full_path)  # Write DataFrame to Parquet files
        )

    def save_data_to_db(self):
        # try boost variants

        # self.statistic_df.write.jdbc(
        #     url=POSTGRES_URL,
        #     table=POSTGRES_TABLE,
        #     mode="append",
        #     properties=PROPERTIES
        # )

        (
            self.statistic_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", POSTGRES_TABLE)
            .option("user", "airflow")
            .option("password", "airflow")
            .save()
        )


if __name__ == "__main__":
    """Only For local development"""

    spark = Spark()
    spark.session = "airflow_task"
    spark.convert_bitfinex_df()
    spark.convert_poloniex_df()
    spark.get_union_df()
    spark.save_data_to_bucket()
    spark.get_statistic_df()
    spark.save_data_to_db()
