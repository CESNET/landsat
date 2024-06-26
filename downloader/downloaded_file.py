import json
import logging
import mimetypes
import os
import tarfile
import time

import requests
import re

from urllib.parse import urlparse, urlunsplit
from pathlib import Path

import stactools.landsat.mtl_metadata
from stactools.landsat import stac as stac_landsat

from stac_connector import STACConnector
from s3_connector import S3Connector

from config import landsat_config

from exceptions.downloaded_file import *
import botocore.exceptions


class DownloadedFile:
    # Just a translation table from dataset id to its full name
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

    _data_file_path = None
    _metadata_xml_file_path = None
    _angle_coefficient_file_path = None
    _pregenerated_stac_item_file_path = None
    _feature_json_file_path = None

    _feature_dict = None
    _feature_id = None

    # True if we want to redownload file eventhough it is already downloaded
    _force_redownload_file = False

    _thread_lock = None

    def __init__(
            self,
            attributes=None,
            stac_connector=None, s3_connector=None,
            workdir=None,
            s3_download_host=landsat_config.s3_download_host,
            logger=logging.getLogger("DownloadedFile"),
            thread_lock=None
    ):
        """
        Constructor

        :param attributes: dict of attributes of downloaded file which will be used
            Must include: {
                displayId, entityId, productId, displayId, url, dataset, start, end, geojson
            }
        :param stac_connector: instance of StacConnector which will be used for registering items
        :param s3_connector: instance of S3Connector into which the file will be downloaded
        :param workdir: Path to workdir
        :param s3_download_host: Base URL of S3 download host relay (URL of the computer on which
            ../http_server/main.py is running)
        :param logger: logger
        :param thread_lock: Since instance of DownloadedFile is ran in multiple threads and uses shared resources
            like the stac_connector or s3_connector, the ThreadLocking is nescessary
        """
        self._logger = logger

        if thread_lock is None:
            raise DownloadedFileThreadLockNotSet()
        self._thread_lock = thread_lock

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

        self._feature_id_json_file_path = self._workdir.joinpath(self._display_id + "_featureId.json")

        self.exception_occurred = None

    def __del__(self):
        """
        Destructor, removes all files this Class has created during its life
        :return:
        """
        if self._data_file_path is not None:
            self._data_file_path.unlink(missing_ok=True)

        if self._metadata_xml_file_path is not None:
            self._metadata_xml_file_path.unlink(missing_ok=True)

        if self._angle_coefficient_file_path is not None:
            self._angle_coefficient_file_path.unlink(missing_ok=True)

        if self._pregenerated_stac_item_file_path is not None:
            self._pregenerated_stac_item_file_path.unlink(missing_ok=True)

        if self._feature_json_file_path is not None:
            self._feature_json_file_path.unlink(missing_ok=True)

        if self._feature_id_json_file_path is not None:
            self._feature_id_json_file_path.unlink(missing_ok=True)

    def get_display_id(self):
        return self._display_id

    def _get_s3_bucket_key_of_file(self, filename):
        """
        Method returns corresponding S3 bucket key for given filename

        :param filename: string
        :return: S3 bucket key corresponding to the given filename
        """
        filename = (
            str(filename).  # Filename
            replace(str(self._workdir), "").  # Remove full workdir path, leave only filename
            replace("\\", "").replace("/", "")  # Also remove any unwanted slashes
        )

        # S3 bucket key consist of dataset name and filename
        return f"{self._dataset}/{filename}"

    def process(self):
        """
        Basically main method of DownloadedFile class
        This method downloads the file using self._download_self(), uploads it to S3 storage, creates STAC item and
        registers it to STAC
        :return:
        """

        try:
            # ============================================ DOWNLOADING FILE ============================================
            file_downloaded = None
            while True:
                try:
                    file_downloaded = self._download_self()
                    break

                except DownloadedFileDownloadedFileHasDifferentSize as e:
                    self._logger.error(e)
                    self._logger.error("Redownloading...")
                    continue
            # ==========================================================================================================

            if file_downloaded:
                self._untar_metadata()

                # Uploading data file to S3
                self._s3_connector.upload_file(
                    local_file=self._data_file_path,
                    bucket_key=self._get_s3_bucket_key_of_file(self._data_file_path)
                )

                # Uploading metadata file to S3
                self._s3_connector.upload_file(
                    local_file=self._metadata_xml_file_path,
                    bucket_key=self._get_s3_bucket_key_of_file(self._metadata_xml_file_path)
                )

                # Uploading angle coefficient file to S3
                if self._angle_coefficient_file_path is not None:
                    self._s3_connector.upload_file(
                        local_file=self._angle_coefficient_file_path,
                        bucket_key=self._get_s3_bucket_key_of_file(self._angle_coefficient_file_path)
                    )

                # Uploading angle coefficient file to S3
                if self._pregenerated_stac_item_file_path is not None:
                    self._s3_connector.upload_file(
                        local_file=self._pregenerated_stac_item_file_path,
                        bucket_key=self._get_s3_bucket_key_of_file(self._pregenerated_stac_item_file_path)
                    )

            else:
                # File is already downloaded in S3, just regenerate feature JSON and re-register it
                try:
                    self._download_feature_from_s3()
                    self._untar_metadata()

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        self._logger.error(e)
                        self._logger.error("We need to re-download file again from USGS")

                        self._force_redownload_file = True  # Setting the _force_redownload_file flag to True
                        self.process()  # Running again self.process method to process the file again
                        return  # After processing the file again return from this method to prevent never-ending loop

            # Preparing STAC feature JSON, uploading it to S3, and registering to STAC
            self._prepare_stac_feature_structure()

            self._s3_connector.upload_file(
                local_file=self._feature_json_file_path,
                bucket_key=self._get_s3_bucket_key_of_file(self._feature_json_file_path)
            )
            self._feature_id = self._stac_connector.register_stac_item(self._feature_dict, self._dataset)
            self._save_feature_id()

        except Exception as exception:
            self.exception_occurred = exception

    def _check_if_already_downloaded(self, expected_length):
        """
        Method checks whether this file already exists on S3 storage.

        :param expected_length: [int] Expected lenght of file in bytes
        :return: True if file exists and its size on storage equals to expected_lenght, otherwise False
        """

        # We forced to re-download file from USGS, thus returning False
        if self._force_redownload_file:
            return False

        return (
            self._s3_connector.check_if_key_exists(
                bucket_key=self._get_s3_bucket_key_of_file(self._filename),
                expected_length=expected_length
            )  # Checks whether the data file itself exists
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
                f"File {self._get_s3_bucket_key_of_file(self._filename)} has been already downloaded."
            )
            return False

        # Creating path for datafile
        self._data_file_path = self._workdir.joinpath(self._filename)
        self._data_file_path.touch(exist_ok=True)

        self._logger.info(f"Downloading {self._url} into {str(self._data_file_path)}.")

        # Iterating through the content of downloaded file and writing it into self._data_file
        with open(self._data_file_path, mode='wb') as result_file:
            for chunk in response.iter_content(chunk_size=(1024 * 1024)):
                result_file.write(chunk)

        # Checking whether the downloaded size is the same as expected size (Content-Length returned by download server)
        expected_size = int(response.headers['Content-Length'])
        real_size = os.stat(str(self._data_file_path)).st_size
        if expected_size != real_size:
            self._data_file_path.unlink(missing_ok=False)
            raise DownloadedFileDownloadedFileHasDifferentSize(
                expected_size=expected_size, real_size=real_size,
                display_id=self._display_id
            )

        return True

    def _untar_metadata(self):
        """
        Method untars metadata needed for creating STAC item
        :return: None
        """
        metadata_xml = self._display_id + "_MTL.xml"

        angle_coefficient_present = True
        angle_coefficient = self._display_id + "_ANG.txt"

        pregenerated_stac_item_present = True
        pregenerated_stac_item = self._display_id + "_stac.json"

        with tarfile.open(name=self._data_file_path) as tar:
            try:
                tar.extract(metadata_xml, self._workdir)
            except KeyError:
                raise DownloadedFileDoesNotContainMetadata(display_id=self._display_id)

            try:
                tar.extract(angle_coefficient, self._workdir)
            except KeyError:
                angle_coefficient_present = False

            try:
                tar.extract(pregenerated_stac_item, self._workdir)
            except KeyError:
                pregenerated_stac_item_present = False

        self._metadata_xml_file_path = self._workdir.joinpath(metadata_xml)

        if angle_coefficient_present:
            self._angle_coefficient_file_path = self._workdir.joinpath(angle_coefficient)

        if pregenerated_stac_item_present:
            self._pregenerated_stac_item_file_path = self._workdir.joinpath(pregenerated_stac_item)

    def _download_feature_from_s3(self):
        """
        Method downloads data file from S3
        :return: None
        """

        self._data_file_path = self._workdir.joinpath(f"{self._display_id}.tar")
        self._s3_connector.download_file(
            self._data_file_path,
            self._get_s3_bucket_key_of_file(self._data_file_path)
        )

    def _dump_stac_feature_into_json(self, feature_dict=None):
        """
        Methods creates JSON file from STAC item dictionary

        :param feature_dict: feature_dictionary, if none then using self._feature_dict
        :return: None, Path to file saved in self._feature_json_file
        """

        if feature_dict is not None:
            self._feature_dict = feature_dict

        self._feature_json_file_path = self._workdir.joinpath(self._display_id + "_feature.json")

        with open(self._feature_json_file_path, "w") as feature_json_file:
            feature_json_file.write(json.dumps(self._feature_dict))

    def _stac_item_clear(self, stac_item_dict):
        """
        Method removes unnecessary items from STAC item, leaving only XML Metadata asset and DATA asset

        :param stac_item_dict: altered STAC item dictionary
        :return:
        """

        stac_item_dict['assets'].clear()
        stac_item_dict['links'].clear()

        """
        if 'thumbnail' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('thumbnail')

        if 'reduced_resolution_browse' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('reduced_resolution_browse')

        if 'index' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('mtl.json')

        if 'mtl.json' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('mtl.json')

        if 'mtl.txt' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('mtl.txt')

        if 'ang' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('ang')

        if 'qa_pixel' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('qa_pixel')

        if 'qa_radsat' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('qa_radsat')

        if 'qa_aerosol' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('qa_aerosol')

        if 'coastal' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('coastal')

        if 'blue' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('blue')

        if 'green' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('green')

        if 'red' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('red')

        if 'nir08' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('nir08')

        if 'swir16' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('swir16')

        if 'swir22' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('swir22')

        if 'nir09' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('nir09')

        if 'atmos_opacity' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('atmos_opacity')

        if 'cloud_qa' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('cloud_qa')

        if 'lwir' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('lwir')

        if 'lwir11' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('lwir11')

        if 'atran' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('atran')

        if 'cdist' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('cdist')

        if 'drad' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('drad')

        if 'urad' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('urad')

        if 'trad' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('trad')

        if 'emis' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('emis')

        if 'emsd' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('emsd')

        if 'qa' in stac_item_dict['assets'].keys():
            stac_item_dict['assets'].pop('qa')
        """

    def _prepare_stac_feature_structure(self):
        """
        Method generates STAC item dictionary

        :return: None, method saves STAC item into self._feature_dict
            and also Path to JSON file into self._feature_json_file
        """

        try:
            self._logger.info("Trying to generate STAC item using stactools.")
            stac_item_dict = (stac_landsat.create_item(str(self._metadata_xml_file_path))
                              .to_dict(include_self_link=False))

        except stactools.landsat.mtl_metadata.MTLError:
            self._logger.info("stactools were unable to create STAC item, using pre-generated STAC item.")
            with open(self._pregenerated_stac_item_file_path, 'r') as pregenerated_stac_item_file:
                stac_item_dict = json.loads(pregenerated_stac_item_file.read())

        self._stac_item_clear(stac_item_dict)

        stac_item_dict['assets'].update(
            {
                'mtl.xml': {
                    'href': urlunsplit(
                        (
                            self._download_host.scheme,
                            self._download_host.netloc,
                            f"{self._get_s3_bucket_key_of_file(self._metadata_xml_file_path)}",
                            "", ""
                        )
                    )
                },
                'data': {
                    'href': urlunsplit(
                        (
                            self._download_host.scheme,
                            self._download_host.netloc,
                            f"{self._get_s3_bucket_key_of_file(self._data_file_path)}",
                            "", ""
                        )
                    ),
                    'type': mimetypes.guess_type(str(self._data_file_path))[0],
                    'title': self._dataset_fullname[self._dataset],
                    'description': f"{self._dataset_fullname[self._dataset]} full data tarball."
                }
            }
        )

        """
        with self._thread_lock:
            from stac_templates.feature import feature
            local_feature = feature
            local_feature['features'] = [stac_item_dict]
            self._feature_dict = local_feature
        """
        self._feature_dict = {
            "type": "FeatureCollection",
            "features": [stac_item_dict]
        }
        self._dump_stac_feature_into_json()

    def _save_feature_id(self):
        """
        Method generates self._feature_id_json_file with information about STAC feature id assigned to this file and
        saves it to S3 storage

        :return: None
        """

        feature_id_json_dict = {
            'displayId': self._display_id,
            'dataset': self._dataset,
            'featureId': self._feature_id
        }

        with open(self._feature_id_json_file_path, "w") as feature_id_json_file:
            feature_id_json_file.write(json.dumps(feature_id_json_dict))

        self._s3_connector.upload_file(
            local_file=self._feature_id_json_file_path,
            bucket_key=self._get_s3_bucket_key_of_file(self._feature_id_json_file_path)
        )
