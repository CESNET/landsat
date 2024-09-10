import json
import logging
import mimetypes
import os
import tarfile
from tempfile import TemporaryDirectory

import numpy as np
import rasterio
import requests
import re

from urllib.parse import urlparse, urlunsplit
from pathlib import Path
from PIL import Image
from skimage import exposure

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
    _workdir_temp: TemporaryDirectory
    _workdir: Path

    _data_file_path = None
    _metadata_xml_file_path = None
    _angle_coefficient_file_path = None
    _pregenerated_stac_item_file_path = None
    _feature_json_file_path = None

    _feature_dict = None
    _feature_id = None

    # False if we want to for exapmle check size of already downloaded files against M2M API
    _catalogue_only = False

    # True if we want to redownload file eventhough it is already downloaded
    _force_redownload_file = False

    _thread_lock = None

    def __init__(
            self,
            attributes=None,
            stac_connector=None, s3_connector=None,
            s3_download_host=landsat_config.s3_download_host,
            logger=logging.getLogger("DownloadedFile"),
            thread_lock=None,
            catalogue_only=landsat_config.catalogue_only,
            force_redownload_file=landsat_config.force_redownload_file
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
        :param thread_lock: Since instance of DownloadedFile is run in multiple threads and uses shared resources
            like the stac_connector or s3_connector, the ThreadLocking is nescessary
        """
        self._logger = logger

        if thread_lock is None:
            raise DownloadedFileThreadLockNotSet()
        self._thread_lock = thread_lock

        if attributes is None:
            raise DownloadedFileWrongConstructorArgumentsPassed()

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

        self._catalogue_only = catalogue_only
        self._force_redownload_file = force_redownload_file

        self._stac_connector = stac_connector
        self._s3_connector = s3_connector

        self._download_host = urlparse(s3_download_host)

        self._workdir_temp = TemporaryDirectory()
        self._workdir = Path(self._workdir_temp.name)
        self._feature_id_json_file_path = self._workdir.joinpath(self._display_id + "_featureId.json")

        self.exception_occurred = None

    def __del__(self):
        """
        Destructor, removes the temporary workdir
        :return:
        """
        if self._workdir_temp:
            self._workdir_temp.cleanup()

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
                # Uploading data file to S3
                self._s3_connector.upload_file(
                    local_file=self._data_file_path,
                    bucket_key=self._get_s3_bucket_key_of_file(self._data_file_path)
                )

            else:
                # File should already be downloaded in S3, just regenerate feature JSON and re-register it
                try:
                    self._download_feature_from_s3()

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        self._logger.error(e)
                        self._logger.error("We need to re-download file again from USGS")

                        self._force_redownload_file = True  # Setting the _force_redownload_file flag to True
                        self.process()  # Running again self.process method to process the file again
                        return  # After processing the file again return from this method to prevent never-ending loop

            self._untar_metadata()

            # Uploading metadata file to S3
            self._upload_metadata_xml_to_s3()

            # Creating STAC feature JSON
            self._create_feature()

            # Generating thumbnail
            self._generate_thumbnail()
            self._upload_thumbnail_to_s3()

            # Adding assests (data, metadata, thumbnail...) to feature
            self._append_assets_to_feature()

            # Registering feature to STAC
            self._feature_id = self._stac_connector.register_stac_item(
                json_dict=self._feature_dict, collection=self._dataset
            )

            # Saving feature dictionary to JSON file
            self._dump_feature_into_json()

            # Uploading feature JSON file and JSON file containing feature_id to S3 storage
            self._upload_feature_to_s3()
            self._upload_feature_id_to_s3()

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

        if (not self._catalogue_only) and (expected_size != real_size):
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
        metadata_xml = f"{self._display_id}_MTL.xml"

        angle_coefficient_present = True
        angle_coefficient = f"{self._display_id}_ANG.txt"

        pregenerated_stac_item_present = True
        pregenerated_stac_item = f"{self._display_id}_stac.json"

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

    def _dump_feature_into_json(self):
        """
        Methods creates JSON file from STAC item dictionary
        Result will be saved in self._feature_json_file_path
        """
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

    def _create_feature(self):
        """
        Method generates STAC item dictionary

        :return: None, method saves STAC item into self._feature_dict
            and also Path to JSON file into self._feature_json_file
        """

        try:
            self._logger.info("Trying to generate STAC item using stactools.")
            stac_item_dict = (stac_landsat.create_item(str(self._metadata_xml_file_path))
                              .to_dict(include_self_link=False))

        except Exception as stactools_exception:
            self._logger.warning("stactools were unable to create STAC item, using pre-generated STAC item.")
            if self._pregenerated_stac_item_file_path is not None:
                with open(self._pregenerated_stac_item_file_path, 'r') as pregenerated_stac_item_file:
                    stac_item_dict = json.loads(pregenerated_stac_item_file.read())
            else:
                raise DownloadedFileCannotCreateStacItem(
                    f"Unable to create STAC item. stactools.landsat exception: {str(stactools_exception)}, " +
                    f"pregenerated STAC item does not exists!"
                )

        self._stac_item_clear(stac_item_dict)

        self._feature_dict = stac_item_dict

    def _append_assets_to_feature(self):
        self._feature_dict['assets'].update(
            {
                'mtl.xml': {
                    'href': urlunsplit(
                        (
                            self._download_host.scheme,
                            self._download_host.netloc,
                            f"{self._get_s3_bucket_key_of_file(self._metadata_xml_file_path)}",
                            "", ""
                        )
                    ),
                    'type': mimetypes.guess_type(str(self._metadata_xml_file_path))[0],
                    'title': f"Metadata",
                    'description': f"Metadata for {self._dataset_fullname[self._dataset]} item {self._display_id}."
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
                    'title': f"Data",
                    'description': f"{self._dataset_fullname[self._dataset]} full data tarball for item {self._display_id}."
                },
                'thumbnail': {
                    'href': urlunsplit(
                        (
                            self._download_host.scheme,
                            self._download_host.netloc,
                            f"{self._get_s3_bucket_key_of_file(self._thumbnail_file_path)}",
                            "", ""
                        )
                    ),
                    'type': mimetypes.guess_type(str(self._thumbnail_file_path))[0],
                    'title': f"Thumbnail",
                    'description': f"Thumbnail for {self._dataset_fullname[self._dataset]} item {self._display_id}."
                }
            }
        )

    def _upload_metadata_xml_to_s3(self):
        self._s3_connector.upload_file(
            local_file=self._metadata_xml_file_path,
            bucket_key=self._get_s3_bucket_key_of_file(self._metadata_xml_file_path)
        )

    def _upload_thumbnail_to_s3(self):
        self._s3_connector.upload_file(
            local_file=self._thumbnail_file_path,
            bucket_key=self._get_s3_bucket_key_of_file(self._thumbnail_file_path)
        )

    def _upload_feature_to_s3(self):
        self._s3_connector.upload_file(
            local_file=self._feature_json_file_path,
            bucket_key=self._get_s3_bucket_key_of_file(self._feature_json_file_path)
        )

    def _upload_feature_id_to_s3(self):
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

    @staticmethod
    def _normalize(array):
        array_min, array_max = array.min(), array.max()
        return (array - array_min) / (array_max - array_min)

    @staticmethod
    def _linear_stretch(array, p_low=2, p_high=98):
        p2, p98 = np.percentile(array, (p_low, p_high))
        return exposure.rescale_intensity(array, in_range=(p2, p98))

    @staticmethod
    def _gamma_correction(image, gamma=1.0):
        return np.power(image, gamma)

    @staticmethod
    def _replace_tif_to_jpg(filename):
        return (
            filename
            .replace('.tif', '.jpg')
            .replace('.TIF', '.jpg')
            .replace('.tiff', '.jpg')
            .replace('.TIFF', '.jpg')
        )

    def _combine_tifs(self, red_path, green_path, blue_path, out_path, size=None):
        self._logger.info(f"Combining bands into thumbnail {out_path}.")

        red = rasterio.open(red_path).read(1)
        green = rasterio.open(green_path).read(1)
        blue = rasterio.open(blue_path).read(1)

        red = self._normalize(red)
        green = self._normalize(green)
        blue = self._normalize(blue)

        red = self._linear_stretch(red)
        green = self._linear_stretch(green)
        blue = self._linear_stretch(blue)

        rgb = np.dstack((red, green, blue))
        rgb = self._gamma_correction(rgb, gamma=0.8)

        image = Image.fromarray((rgb * 255).astype(np.uint8))
        if size is not None:
            image = image.resize(size, Image.Resampling.LANCZOS)

        out_path = Path(self._replace_tif_to_jpg(str(out_path)))
        image.save(out_path, 'JPEG', quality=90)

        return out_path

    def _generate_thumbnail(self):
        with tarfile.open(self._data_file_path, mode='r') as tar:
            blue_tif_filename = None
            green_tif_filename = None
            red_tif_filename = None
            nir_tif_filename = None

            match self._dataset:
                case "landsat_ot_c2_l1" | "landsat_ot_c2_l2":
                    blue_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B2' in filename.upper()),
                        None
                    )
                    green_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B3' in filename.upper()),
                        None
                    )
                    red_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B4' in filename.upper()),
                        None
                    )

                case "landsat_etm_c2_l1" | "landsat_etm_c2_l2" | "landsat_tm_c2_l1" | "landsat_tm_c2_l2":
                    blue_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B1' in filename.upper()),
                        None
                    )
                    green_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B2' in filename.upper()),
                        None
                    )
                    red_tif_filename = next(
                        (filename for filename in tar.getnames() if 'B3' in filename.upper()),
                        None
                    )

                case "landsat_mss_c2_l1":
                    match self._feature_dict['properties']['platform']:
                        case "landsat-1" | "landsat-2" | "landsat-3":
                            green_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B4' in filename.upper()),
                                None
                            )
                            red_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B5' in filename.upper()),
                                None
                            )
                            nir_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B6' in filename.upper()),
                                None
                            )

                        case "landsat-4" | "landsat-5":
                            green_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B1' in filename.upper()),
                                None
                            )
                            red_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B2' in filename.upper()),
                                None
                            )
                            nir_tif_filename = next(
                                (filename for filename in tar.getnames() if 'B3' in filename.upper()),
                                None
                            )

                        case _:
                            raise ValueError(
                                f"Unexpected platform: {self._feature_dict['properties']['platform']}!")
                case _:
                    raise ValueError(f"Unexpected dataset {self._dataset}!")

            output_filename = f"{self._display_id}_thumbnail.jpg"

            if blue_tif_filename and green_tif_filename and red_tif_filename:
                tar.extract(blue_tif_filename, path=self._workdir)
                blue_tif_path = self._workdir.joinpath(blue_tif_filename)
                tar.extract(green_tif_filename, path=self._workdir)
                green_tif_path = self._workdir.joinpath(green_tif_filename)
                tar.extract(red_tif_filename, path=self._workdir)
                red_tif_path = self._workdir.joinpath(red_tif_filename)

                output_path = self._combine_tifs(
                    red_path=red_tif_path,
                    green_path=green_tif_path,
                    blue_path=blue_tif_path,
                    out_path=self._workdir.joinpath(output_filename),
                    size=(1000, 1000)
                )

            elif green_tif_filename and red_tif_filename and nir_tif_filename:
                tar.extract(green_tif_filename, path=self._workdir)
                green_tif_path = self._workdir.joinpath(green_tif_filename)
                tar.extract(red_tif_filename, path=self._workdir)
                red_tif_path = self._workdir.joinpath(red_tif_filename)
                tar.extract(nir_tif_filename, path=self._workdir)
                nir_tif_path = self._workdir.joinpath(nir_tif_filename)

                output_path = self._combine_tifs(
                    red_path=red_tif_path,
                    green_path=green_tif_path,
                    blue_path=nir_tif_path,
                    out_path=self._workdir.joinpath(output_filename),
                    size=(1000, 1000)
                )

            else:
                raise ValueError(f"Thumbnail suitable data not found!")

        self._thumbnail_file_path = output_path
