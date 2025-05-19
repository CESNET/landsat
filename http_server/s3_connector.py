import boto3
import logging

import botocore.exceptions

from pathlib import Path

import config.variables as variables

class S3Connector:
    def __init__(
            self,
            logger=logging.getLogger("S3Connector"),
            service_name='s3',
            s3_endpoint=variables.S3_CONNECTOR__HOST_BASE,
            access_key=variables.S3_CONNECTOR__CREDENTIALS['access_key'],
            secret_key=variables.S3_CONNECTOR__CREDENTIALS['secret_key'],
            host_bucket=variables.S3_CONNECTOR__HOST_BUCKET
    ):
        """
        Constructor of S3Connector class

        :param logger:
        :param service_name:
        :param s3_endpoint:
        :param access_key:
        :param secret_key:
        :param host_bucket:
        """
        self._logger = logger
        self._s3_client = boto3.client(
            service_name=service_name,
            endpoint_url=s3_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self._bucket = host_bucket

    def get_bucket(self):
        return self._bucket

    def get_s3_client(self):
        return self._s3_client

    def upload_file(self, local_file, bucket_key):
        """
        Uploads local_file to S3 storage as host_bucket/bucket_key

        :param local_file: Absolute path to local file
        :param bucket_key: bucket_key
        :return: nothing
        """

        local_file = str(local_file)
        self._logger.info(f"Uploading file={local_file} to S3 as key={bucket_key}.")
        self._s3_client.upload_file(local_file, self._bucket, bucket_key)

    def download_file(self, path_to_download, bucket_key):
        """
        Method downloads file from S3 storage into local file

        :param path_to_download: absolute Path to local file into which file is downloaded
        :param bucket_key: key of downloaded file. It will be used as follows: host_bucket/bucket_key
        :return: nothing
        :raise: botocore.exceptions.ClientError
        """
        self._logger.info(f"Downloading S3 key={bucket_key} into file={str(path_to_download)}.")

        Path(path_to_download).unlink(missing_ok=True)
        Path(path_to_download).parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(path_to_download, 'wb') as downloaded_file:
                self._s3_client.download_fileobj(self._bucket, bucket_key, downloaded_file)

        except botocore.exceptions.ClientError as e:
            raise e

    def delete_key(self, bucket_key):
        """
        Deletes key from S3 storage

        :param bucket_key: deleted key, used as follows host_bucket/bucket_key
        :return: nothing
        """
        self._logger.info(f"Deleting S3 key={bucket_key}.")
        self._s3_client.delete_object(Bucket=self._bucket, Key=bucket_key)

    def check_if_key_exists(self, bucket_key, expected_length=None):
        """
        Method checks whether this file already exists on S3 storage.

        :param bucket_key: S3 key of the checked file
        :param expected_length: [int] Expected lenght of file in bytes, or None if we do not want to check size
        :return: True if file exists and its size on storage equals to expected_lenght, otherwise False
        :raise: botocore.exceptions.ClientError for every error other than HTTP/404
        """

        bucket_key = str(bucket_key)

        try:
            key_head = self._s3_client.head_object(Bucket=self._bucket, Key=bucket_key)
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
                self._logger.warning(
                    f"S3 key {bucket_key} length ({key_head['ContentLength']} b) does not match expected length " +
                    f"({expected_length} b)"
                )
                self.delete_key(bucket_key)
                return False
        else:
            # We do not have to check sizes, file exists and that's enough
            return True

    def list_files(self, directory_path):
        response = self._s3_client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=directory_path.rstrip('/') + '/',
        )
        # Check if any content is returned
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        else:
            return []

    def get_file_object(self, key):
        return self._s3_client.get_object(Bucket=self._bucket, Key=key)

    def fetch_from_tar_by_range(self, key:str, offset, size):
        byte_range = f"bytes={offset}-{offset + size - 1}"
        response = self._s3_client.get_object(Bucket=self._bucket, Key=key, Range=byte_range)
        return response['Body'].read()

    def generate_fileshare_url(self, key):
        url = self._s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._bucket,
                'Key': key
            }
        )
        return url
