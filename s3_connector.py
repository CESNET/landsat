import boto3
import logging

import botocore.exceptions

import config.s3_config as s3_config

"""
Create file ./downloader/config/s3_config.py with following content:

host_base = "https://s3.cl4.du.cesnet.cz"
use_https = True
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

    def upload_file(self, local_filename, bucket_key):
        self.logger.info("Uploading file=" + local_filename + " to S3 as key=" + bucket_key + ".")

        # TODO Pokud by bylo paralelizovano, tak zde musi byt nejaky mutex
        self.s3_client.upload_file(local_filename, self.bucket, bucket_key)

    def download_file(self, path_to_download, bucket_key):
        self.logger.info("Downloading key=" + bucket_key + " into file=" + path_to_download + ".")

        with open(path_to_download, 'wb') as downloaded_file:
            self.s3_client.download_fileobj(self.bucket, bucket_key, downloaded_file)

    def delete_key(self, key):
        self.logger.info("Deleting key=" + key + ".")

        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    """def _update_keys(self):
        \"""
        Method updates array of keys which are representing files in S3 storage.
        This is done by using pagination, since the total number of keys one page is able to return is 1000.

        :return: Nothing, but method alters (upadtes) array self.keys; and boolean self.keys_up_to_date to True
        \"""
        # TODO Pokud by bylo paralelizovano, tak zde musi byt nejaky mutex
        self.keys = []

        paginator = self.s3_client.get_paginator('list_objects')
        operation_parameters = {'Bucket': self.bucket, 'Prefix': ''}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for content_object in page['Contents']:
                self.keys.append(content_object['Key'])

        self.keys_up_to_date = True
        # self.logger.info(len(self.keys))
        """

    def check_if_key_exists(self, key, expected_size):
        try:
            key_head = self.s3_client.head_object(Bucket=self.bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code']=="404":
                # File/key does not exist
                return False
            else:
                # This can be HTTP 403, or other error
                raise e

        # File exists...

        if str(key_head['ContentLength']) == expected_size:
            # ...and have the right size
            return True
        else:
            # ...but does not have the right size. Let's delete this key and download it again.
            self.delete_key(key)
            return False

        
        """if not self.keys_up_to_date:
            self._update_keys()

        return key in self.get_contents_keys()"""
