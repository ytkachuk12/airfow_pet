"""
Interaction with S3 Minio.
    - creating buckets
    - writing data to buckets
    - getting data from buckets
    - deleting data from buckets
"""

import json
import io
import os
import requests
import sys
from datetime import datetime
from typing import Union, List

from minio import Minio
from minio.error import S3Error

from core.constants import (
    MINIO_URL_S3,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BITFINEX_BUCKET,
    BITFINEX_URL,
    POLONIEX_BUCKET,
    POLONIEX_URL,
    DATA_BUCKET
)


class MinioClient:
    """
    Interaction with S3 Minio.
        - creating buckets
        - writing data to buckets
        - getting data from buckets
        - deleting data from buckets
    """

    bitfinex_bucket = BITFINEX_BUCKET
    bitfinex_url = BITFINEX_URL
    poloniex_bucket = POLONIEX_BUCKET
    poloniex_url = POLONIEX_URL

    data_bucket = DATA_BUCKET

    def __init__(self):
        self.minio_client = None
        self.message = None

    def get_or_create_minio_client(self) -> None:
        """Creating minio_client with access key and secret key if it does not exist"""

        if not self.minio_client:
            # Create client with access key and secret key
            self.minio_client = Minio(
                endpoint=MINIO_URL_S3,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False,
            )

    def get_or_create_bucket(self, bucket_name: str) -> str:
        """Creating bucket if it does not exist"""

        if self.minio_client.bucket_exists(bucket_name):
            return "exists"

        self.minio_client.make_bucket(bucket_name)
        return "created"

    def put_data_into_bucket(self, bucket_name: str, url: str) -> Union[str, S3Error]:
        """Get data from url and put it into Minio bucket"""

        # get data from url
        r = requests.get(url)
        if bucket_name == self.poloniex_bucket:
            # cose \n symbols
            data = r.json()
            data = json.dumps(data, ensure_ascii=False).encode('utf-8')
        else:
            data = r.content

        # put data into minio
        try:
            self.minio_client.put_object(
                bucket_name=bucket_name,
                # object name is datetime moment of creation
                object_name=datetime.now().isoformat() + ".json",
                data=io.BytesIO(data),
                # I don`t now why - 33 byte, but else it`s throw exp
                length=sys.getsizeof(data) - 33,
                content_type="application/json",
            )
            return "Successes"
        except S3Error as exc:
            return exc

    def _get_list_objects(self, bucket_name: str) -> List[str]:
        """Get list of all objects contained in the basket"""

        # List objects information.
        objects = self.minio_client.list_objects(bucket_name)
        print(type(objects))
        return objects

    def _delete_object(self, bucket_name: str, file_name: str):
        self.minio_client.remove_object(bucket_name, file_name)

    def delete_objects(self, bucket_name):
        list_objects = self._get_list_objects(bucket_name)
        for file in list_objects:
            self._delete_object(bucket_name, file.object_name)
