from stac_connector import STACConnector
from s3_connector import S3Connector

from exceptions.downloaded_file import *


class DownloadedFile:
    _stac_connector: STACConnector
    _s3_connector: S3Connector

    def __init__(self, attributes=None, stac_connector=None, s3_connector=None, workdir=None):
        if attributes is None:
            raise DownloadedFileWrongConstructorArgumentsPassed()

        if workdir is None:
            raise DownloadedFileWorkdirNotSpecified(downloaded_file_display_id=attributes['displayId'])

        if s3_connector is None:
            raise DownloadedFileS3ConnectorNotSpecified(downloaded_file_display_id=attributes['displayId'])

        if stac_connector is None:
            raise DownloadedFileSTACConnectorNotSpecified(downloaded_file_display_id=attributes['displayId'])

        self._entity_id = attributes['entityId']
        self._product_id = attributes['productId']
        self._display_id = attributes['displayId']
        self._url = attributes['url']
        self._dataset = attributes['dataset']
        self._date_start = attributes['start']
        self._date_end = attributes['end']

        self._stac_connector = stac_connector
        self._s3_connector = s3_connector

    def process(self):
        self._download_self()

    def _download_self(self):
