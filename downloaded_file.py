import json
import logging
import mimetypes
import os
import tarfile
import requests
import re

from urllib.parse import urlparse, urlunsplit
from pathlib import Path

from stactools.landsat import stac as stac_landsat

from stac_connector import STACConnector
from s3_connector import S3Connector

from config import landsat_config
from exceptions.downloaded_file import *

import utils


class DownloadedFile:
    _dataset_fullname = {
        "landsat_ot_c2_l1": "Landsat 8-9 OLI/TIRS C2 L1",
        "landsat_ot_c2_l2": "Landsat 8-9 OLI/TIRS C2 L2",
        "landsat_etm_c2_l1": "Landsat 7 ETM+ C2 L1",
        "landsat_etm_c2_l2": "Landsat 7 ETM+ C2 L2",
        "landsat_tm_c2_l1": "Landsat 4-5 TM C2 L1",
        "landsat_tm_c2_l2": "Landsat 4-5 TM C2 L2",
        "landsat_mss_c2_l1": "Landsat 1-5 MSS C2 L1"
    }

    _stac_connector: STACConnector
    _s3_connector: S3Connector
    _workdir: Path

    _data_file = None
    _metadata_txt_file = None
    _metadata_xml_file = None
    _angle_coefficient_file = None
    _feature_json_file = None

    _feature_dict = None
    _feature_id = None

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
        self._download_host = urlparse(s3_download_host)

        self._feature_id_json_file = self._workdir.joinpath(self._display_id + "_featureId.json")

    def __del__(self):
        if self._data_file is not None:
            self._data_file.unlink(missing_ok=True)

        if self._metadata_txt_file is not None:
            self._metadata_txt_file.unlink(missing_ok=True)

        if self._metadata_xml_file is not None:
            self._metadata_xml_file.unlink(missing_ok=True)

        if self._angle_coefficient_file is not None:
            self._angle_coefficient_file.unlink(missing_ok=True)

        if self._feature_json_file is not None:
            self._feature_json_file.unlink(missing_ok=True)

        if self._feature_id_json_file is not None:
            self._feature_id_json_file.unlink(missing_ok=True)

    def _get_s3_bucket_key_of_attribute(self, attribute):
        attribute = (
            str(attribute).  # Attribute
            replace(str(self._workdir), "").  # Remove full workdir path, leave only filename
            replace("\\", "").replace("/", "")  # Also remove any unwanted slashes
        )
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

            # Uploading data file and metadata to S3
            self._s3_connector.upload_file(
                local_file=self._data_file,
                bucket_key=self._get_s3_bucket_key_of_attribute(self._data_file)
            )
            self._s3_connector.upload_file(
                local_file=self._metadata_txt_file,
                bucket_key=self._get_s3_bucket_key_of_attribute(self._metadata_txt_file)
            )
            self._s3_connector.upload_file(
                local_file=self._metadata_xml_file,
                bucket_key=self._get_s3_bucket_key_of_attribute(self._metadata_xml_file)
            )

            # Preparing STAC feature JSON, uploading it to S3, and registering to STAC
            self._prepare_stac_feature_structure()

        else:
            # File is already downloaded in S3, just regenerate feature JSON and re-register it
            self._download_feature_metadata_xml()
            self._download_feature_angle_coefficient()
            self._prepare_stac_feature_structure()
            # self._update_json_feature()

        self._s3_connector.upload_file(
            local_file=self._feature_json_file,
            bucket_key=self._get_s3_bucket_key_of_attribute(self._feature_json_file)
        )
        self._feature_id = self._stac_connector.register_stac_item(self._feature_dict, self._dataset)
        self._save_feature_id()

    def _check_if_already_downloaded(self, expected_length):
        """
        Method checks whether this file already exists on S3 storage.

        :param expected_length: [int] Expected lenght of file in bytes
        :return: True if file exists and its size on storage equals to expected_lenght, otherwise False
        """
        exists = (
                     self._s3_connector.check_if_key_exists(
                         bucket_key=self._get_s3_bucket_key_of_attribute(self._filename),
                         expected_length=expected_length
                     )  # Checks whether the data file itself exists
                 ) and (
                     self._s3_connector.check_if_key_exists(
                         bucket_key=self._get_s3_bucket_key_of_attribute(self._feature_id_json_file),
                         expected_length=None
                     )  # Checks whether the featureId is uploaded - that means, that the data is registered to STAC
                 )

        return exists

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
                f"File {self._get_s3_bucket_key_of_attribute(self._filename)} has been already downloaded."
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
        angle_coefficient = self._display_id + "_ANG.txt"

        try:
            with tarfile.open(name=self._data_file) as tar:
                tar.extract(metadata_txt, self._workdir)
                tar.extract(metadata_xml, self._workdir)
                tar.extract(angle_coefficient, self._workdir)
        except KeyError:
            raise DownloadedFileDoesNotContainMetadata(display_id=self._display_id)

        self._metadata_txt_file = self._workdir.joinpath(metadata_txt)
        self._metadata_xml_file = self._workdir.joinpath(metadata_xml)
        self._angle_coefficient_file = self._workdir.joinpath(angle_coefficient)

    def _download_feature_metadata_xml(self):
        filename = self._display_id + "_MTL.xml"
        self._metadata_xml_file = self._workdir.joinpath(filename)

        self._s3_connector.download_file(self._metadata_xml_file, self._get_s3_bucket_key_of_attribute(filename))

    def _download_feature_angle_coefficient(self):
        filename = self._display_id + "_ANG.txt"
        self._angle_coefficient_file = self._workdir.joinpath(filename)

        self._s3_connector.download_file(self._angle_coefficient_file, self._get_s3_bucket_key_of_attribute(filename))

    def _dump_stac_feature_into_json(self, feature_dict=None):
        if feature_dict is not None:
            self._feature_dict = feature_dict

        self._feature_json_file = self._workdir.joinpath(self._display_id + "_feature.json")

        with open(self._feature_json_file, "w") as feature_json_file:
            feature_json_file.write(json.dumps(self._feature_dict))

    def _stac_item_clear(self, stac_item_dict):
        stac_item_dict['assets'].pop('thumbnail')
        stac_item_dict['assets'].pop('reduced_resolution_browse')
        stac_item_dict['assets'].pop('mtl.json')
        stac_item_dict['assets'].pop('qa_pixel')
        stac_item_dict['assets'].pop('qa_radsat')
        stac_item_dict['assets'].pop('coastal')
        stac_item_dict['assets'].pop('blue')
        stac_item_dict['assets'].pop('green')
        stac_item_dict['assets'].pop('red')
        stac_item_dict['assets'].pop('nir08')
        stac_item_dict['assets'].pop('swir16')
        stac_item_dict['assets'].pop('swir22')
        stac_item_dict['assets'].pop('qa_aerosol')

    def _prepare_stac_feature_structure(self):
        stac_item_dict = stac_landsat.create_item(str(self._metadata_xml_file)).to_dict(include_self_link=False)
        self._stac_item_clear(stac_item_dict)

        stac_item_dict['assets']['mtl.txt']['href'] = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                f"{self._get_s3_bucket_key_of_attribute(self._metadata_txt_file)}",
                "", ""
            )
        )

        stac_item_dict['assets']['mtl.xml']['href'] = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                f"{self._get_s3_bucket_key_of_attribute(self._metadata_xml_file)}",
                "", ""
            )
        )

        stac_item_dict['assets']['data']['href'] = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                f"{self._get_s3_bucket_key_of_attribute(self._data_file)}",
                "", ""
            )
        )
        stac_item_dict['assets']['data']['type'] = mimetypes.guess_type(self._data_file)[0]
        stac_item_dict['assets']['data']['title'] = self._dataset_fullname[self._dataset]
        stac_item_dict['assets']['data']['title'] = f"{self._dataset_fullname[self._dataset]} full data tarball."

        from stac_templates.feature import feature
        self._feature_dict = feature['features'] = [stac_item_dict]
        self._dump_stac_feature_into_json()

    def _update_json_feature(self):
        # TODO tahle metoda by už neměla být potřeba
        with open(self._feature_json_file, "r") as feature_json_file:
            self._feature_dict = json.load(feature_json_file)

        metadata_txt_href = urlparse(self._feature_dict['features'][0]['assets']['mtl.txt']['href'])
        metadata_txt_href = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                metadata_txt_href.path.strip('/'),
                "", ""
            )
        )
        self._feature_dict['features'][0]['assets']['mtl.txt']['href'] = metadata_txt_href

        metadata_xml_href = urlparse(self._feature_dict['features'][0]['assets']['mtl.xml']['href'])
        metadata_xml_href = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                metadata_xml_href.path.strip('/'),
                "", ""
            )
        )
        self._feature_dict['features'][0]['assets']['mtl.xml']['href'] = metadata_xml_href

        data_href = urlparse(self._feature_dict['features'][0]['assets']['data']['href'])
        data_href = urlunsplit(
            (
                self._download_host.scheme,
                self._download_host.netloc,
                data_href.path.strip('/'),
                "", ""
            )
        )
        self._feature_dict['features'][0]['assets']['data']['href'] = data_href

        self._dump_stac_feature_into_json()

    def _save_feature_id(self):
        feature_id_json_dict = {
            'displayId': self._display_id,
            'featureId': self._feature_id
        }

        with open(self._feature_id_json_file, "w") as feature_id_json_file:
            feature_id_json_file.write(json.dumps(feature_id_json_dict))

        self._s3_connector.upload_file(
            local_file=self._feature_id_json_file,
            bucket_key=self._get_s3_bucket_key_of_attribute(self._feature_id_json_file)
        )
