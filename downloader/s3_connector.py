import boto3
import logging

import botocore.exceptions

import config.s3_config as s3_config

"""
Create file ./config/s3_config.py with following content:

host_base = "https://s3.cl4.du.cesnet.cz"
access_key = "your_access_key_should_go_here"
secret_key = "your_secret_key_should_go_here"
host_bucket = "landsat"
"""


class S3Connector:
    def __init__(
            self,
            logger=logging.getLogger("S3Connector"),
            service_name='s3',
            s3_endpoint=s3_config.host_base,
            access_key=s3_config.access_key,
            secret_key=s3_config.secret_key,
            host_bucket=s3_config.host_bucket
    ):
        self.logger = logger
        self.s3_client = boto3.client(
            service_name=service_name,
            endpoint_url=s3_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.bucket = host_bucket

    def upload_file(self, local_file, bucket_key):
        local_file = str(local_file)
        self.logger.info(f"Uploading file={local_file} to S3 as key={bucket_key}.")
        self.s3_client.upload_file(local_file, self.bucket, bucket_key)

    def download_file(self, path_to_download, bucket_key):
        self.logger.info(f"Downloading key={bucket_key} into file={str(path_to_download)}.")

        try:
            with open(path_to_download, 'wb') as downloaded_file:
                self.s3_client.download_fileobj(self.bucket, bucket_key, downloaded_file)

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.error(e)
                exit(-1)

    def delete_key(self, bucket_key):
        self.logger.info(f"Deleting S3 key={bucket_key}.")
        self.s3_client.delete_object(Bucket=self.bucket, Key=bucket_key)

    def check_if_key_exists(self, bucket_key, expected_length):
        """
        Method checks whether this file already exists on S3 storage.

        :param bucket_key: S3 key of the checked file
        :param expected_length: [int] Expected lenght of file in bytes, or None if we do not want to check size
        :return: True if file exists and its size on storage equals to expected_lenght, otherwise False
        """

        try:
            key_head = self.s3_client.head_object(Bucket=self.bucket, Key=bucket_key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # File/key does not exist
                return False
            else:
                # This can be HTTP 403, or other error
                raise e

        # File exists...

        if expected_length is not None:
            # We have to check sizes
            if str(key_head['ContentLength']) == expected_length:
                # ...and have the right size
                return True
            else:
                # ...but does not have the right size. Let's delete this key and download it again.
                self.delete_key(bucket_key)
                return False
        else:
            # We do not have to check sizes, file exists and that's enough
            return True
