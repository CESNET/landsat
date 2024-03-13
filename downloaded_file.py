import logging
import mimetypes
import os
import tarfile
import requests
import re

from pathlib import Path

from stac_connector import STACConnector
from s3_connector import S3Connector

from config import landsat_config
from exceptions.downloaded_file import *

import utils


class DownloadedFile:
    _stac_connector: STACConnector
    _s3_connector: S3Connector
    _workdir: Path

    def __init__(
            self,
            attributes=None,
            stac_connector=None, s3_connector=None,
            workdir=None,
            s3_download_host=landsat_config.s3_download_host,
            logger=logging.getLogger("DownloadedFile")
    ):
        self._logger = logger

        if attributes is None:
            raise DownloadedFileWrongConstructorArgumentsPassed()

        if workdir is None:
            raise DownloadedFileWorkdirNotSpecified(display_id=attributes['displayId'])

        if s3_connector is None:
            raise DownloadedFileS3ConnectorNotSpecified(display_id=attributes['displayId'])

        if stac_connector is None:
            raise DownloadedFileSTACConnectorNotSpecified(display_id=attributes['displayId'])

        self._entity_id = attributes['entityId']
        self._product_id = attributes['productId']
        self._display_id = attributes['displayId']
        self._url = attributes['url']
        self._dataset = attributes['dataset']
        self._date_start = attributes['start']
        self._date_end = attributes['end']
        self._geojson = attributes['geojson']

        self._stac_connector = stac_connector
        self._s3_connector = s3_connector

        self._workdir = workdir
        self._download_host = s3_download_host

    def __del__(self):
        self._data_file.unlink(missing_ok=True)
        self._metadata_txt_file.unlink(missing_ok=True)
        self._metadata_xml_file.unlink(missing_ok=True)

    def _get_s3_bucket_key_of_attribute(self, attribute):
        return f"{self._dataset}/{attribute}"

    def process(self):

        # ==================================== DOWNLOADING FILE =======================================================
        file_downloaded = None
        while True:
            try:
                file_downloaded = self._download_self()
                break

            except DownloadedFileDownloadedFileHasDifferentSize as e:
                self._logger.error(e)
                self._logger.error("Redownloading...")
                continue
        # =============================================================================================================

        if file_downloaded:
            self._untar_metadata()
            self._prepare_stac_feature_structure()
        else:
            pass

    def _check_if_already_downloaded(self, expected_length):
        """
        Method checks whether this file already exists on S3 storage.

        :param expected_length: [int] Expected lenght of file in bytes
        :return: True if file exists and its size on storage equals to expected_lenght, otherwise False
        """
        return self._s3_connector.check_if_key_exists(
            bucket_key=self._get_s3_bucket_key_of_attribute(self._filename),
            expected_length=expected_length
        )

    def _download_self(self):
        """
        Method downloads the file which the instance of DownloadedFile represents into local copy (workdir/filename)

        :return: True if the new copy of the file has beed downloaded, False if the file was already found on S3 storage
        """
        self._workdir.mkdir(exist_ok=True)

        response = requests.get(self._url, stream=True)

        self._filename = None
        for content_disposition in response.headers['Content-Disposition'].split(' '):
            if 'filename' in content_disposition:
                self._filename = re.findall(r'"([^"]*)"', content_disposition)[0]

        if self._filename is None:
            raise DownloadedFileUrlDoesNotContainFilename(url=self._url, display_id=self._display_id)

        if self._check_if_already_downloaded(expected_length=response.headers['Content-Length']):
            # Well the file has already been downloaded, so there is no need to download it again and this
            # method cannot succeed in downloading the file that has already been downloaded. Let's return False.
            self._logger.info(
                f"File {self._get_s3_bucket_key_of_attribute(self._filename)} has been already downloaded. Skipping."
            )
            return False

        self._data_file = self._workdir.joinpath(self._filename)
        self._data_file.touch(exist_ok=True)

        self._logger.info(f"Downloading {self._url} into {str(self._data_file)}.")

        with open(self._data_file, mode='wb') as result_file:
            for chunk in response.iter_content(chunk_size=(1024 * 1024)):
                result_file.write(chunk)

        expected_size = int(response.headers['Content-Length'])
        real_size = os.stat(str(self._data_file)).st_size
        if expected_size != real_size:
            self._data_file.unlink(missing_ok=False)
            raise DownloadedFileDownloadedFileHasDifferentSize(
                expected_size=expected_size, real_size=real_size,
                display_id=self._display_id
            )

        return True

    def _untar_metadata(self):
        metadata_txt = self._display_id + "_MTL.txt"
        metadata_xml = self._display_id + "_MTL.xml"

        try:
            with tarfile.open(name=self._data_file) as tar:
                tar.extract(metadata_txt, self._workdir)
                tar.extract(metadata_xml, self._workdir)
        except KeyError:
            raise DownloadedFileDoesNotContainMetadata(display_id=self._display_id)

        self._metadata_txt_file = Path(self._workdir).joinpath(metadata_txt)
        self._metadata_xml_file = Path(self._workdir).joinpath(metadata_xml)

    def _prepare_stac_feature_structure(self):
        from stac_templates.feature import feature

        feature['features'][0]['id'] = self._display_id
        feature['features'][0]['geometry'] = self._geojson
        feature['features'][0]['bbox'] = utils.convert_geojson_to_bbox(self._geojson)
        feature['features'][0]['properties']['datetime'] = self._date_start.isoformat()
        feature['features'][0]['properties']['start_datetime'] = self._date_start.isoformat()
        feature['features'][0]['properties']['end_datetime'] = self._date_end.isoformat()

        feature['features'][0]['assets'].update(
            {
                'title': self._display_id,
                'entity_id': self._entity_id,
                'product_id': self._product_id,
                'metadata': {
                    'txt': {
                        'href': 'http://147.251.115.146:8081/' + self._get_s3_bucket_key_of_attribute(
                            self._metadata_txt_file),
                        'type': mimetypes.guess_type(self._metadata_txt_file)[0]
                    },
                    'xml': {
                        'href': 'http://147.251.115.146:8081/' + self._get_s3_bucket_key_of_attribute(
                            self._metadata_xml_file),
                        'type': mimetypes.guess_type(self._metadata_xml_file)[0]
                    }
                },
                'data': {
                    'href': 'http://147.251.115.146:8081/' + self._get_s3_bucket_key_of_attribute(self._data_file),
                    'type': mimetypes.guess_type(self._data_file)[0]
                }
            }
        )

        self._feature_dict = feature
        
        ## TODO POKRAČOVAT ODTUD

        feature_json = json.dumps(feature)
        feature_json_filepath = Path.joinpath(Path(self._workdir), downloaded_file['displayId'] + "_feature.json")
        with open(feature_json_filepath, "w") as feature_json_file:
            feature_json_file.write(feature_json)

        feature_id = self._stac_connector.register_stac_item(feature_json, downloaded_file['dataset'])
        feature_id = json.dumps({'feature_id': feature_id})
        feature_id_filepath = Path.joinpath(Path(self._workdir), downloaded_file['displayId'] + "_featureId.json")
        with open(feature_id_filepath, "w") as feature_id_file:
            feature_id_file.write(feature_id)

        downloaded_file.update(
            {
                "feature_id_s3_bucket_key": f"{downloaded_file['dataset']}/{downloaded_file['displayId']}_featureId.json",
                "feature_id_file_path": str(feature_id_filepath),
                "feature_json_s3_bucket_key": f"{downloaded_file['dataset']}/{downloaded_file['displayId']}_feature.json",
                "feature_json_file_path": str(feature_json_filepath),
            }
        )
