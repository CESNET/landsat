import json
import logging
import datetime
import os
import re
import mimetypes
import tarfile
from pathlib import Path

import requests

from m2m_api_connector import M2MAPIConnector
from stac_connector import STACConnector
from s3_connector import S3Connector

from downloaded_file import DownloadedFile

from exceptions.landsat_downloader import *


class LandsatDownloader:
    _last_downloaded_day_filename = 'last_downloaded_day.json'

    _demanded_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]

    _dataset_fullname = {
        "landsat_ot_c2_l1": "Landsat 8-9 OLI/TIRS C2 L1",
        "landsat_ot_c2_l2": "Landsat 8-9 OLI/TIRS C2 L2",
        "landsat_etm_c2_l1": "Landsat 7 ETM+ C2 L1",
        "landsat_etm_c2_l2": "Landsat 7 ETM+ C2 L2",
        "landsat_tm_c2_l1": "Landsat 4-5 TM C2 L1",
        "landsat_tm_c2_l2": "Landsat 4-5 TM C2 L2",
        "landsat_mss_c2_l1": "Landsat 1-5 MSS C2 L1"
    }

    def __init__(
            self,
            m2m_username=None, m2m_token=None,
            stac_username=None, stac_password=None,
            root_directory=None, working_directory=None,
            logger=logging.getLogger('LandsatDownloader'),
            feature_download_host=None
    ):
        logger.info("=== DOWNLOADER INITIALIZING ===")

        if root_directory is None:
            raise Exception("root_directory must be specified")

        if working_directory is None:
            raise Exception("working_directory must be specified")

        if feature_download_host is None:
            raise Exception("feature_download_host must be specified")

        self._root_directory = Path(root_directory)
        self._workdir = self._root_directory.joinpath(working_directory)
        self._logger = logger
        self._feature_download_host = feature_download_host

        self._clean_up()

        self._m2m_api_connector = M2MAPIConnector(logger=self._logger)
        self._stac_connector = STACConnector(logger=logger)
        self._s3_connector = S3Connector(logger=logger)

        self._logger.info('=== DOWNLOADER INITIALIZED ===')

    def _clean_up(self):
        """
        Method initializes filesystem structure. Deleting old temp directories and creating new working directory
        specified in folder self.working_directory.

        :return: nothing
        """

        pycache_dir = self._root_directory.joinpath("__pycache__")
        self._logger.info(f"Initial cleanup: Deleting {pycache_dir}")
        pycache_dir.unlink(missing_ok=True)

        import shutil
        self._logger.info(f"Initial cleanup: Deleting {self._workdir}")
        # self._workdir.unlink(missing_ok=True) # Does not work, returns WinError 5: Access Denied
        shutil.rmtree(self._workdir, ignore_errors=False)

        self._logger.info(f"Initial cleanup: Creating directory {self._workdir}")
        self._workdir.mkdir(parents=True, exist_ok=True)

    def _get_last_downloaded_day(self):
        download_to = Path(self._workdir).joinpath(self._last_downloaded_day_filename)

        self._s3_connector.download_file(
            download_to, self._last_downloaded_day_filename
        )

        with open(download_to) as last_downloaded_day_file:
            last_downloaded_day = datetime.datetime.strptime(
                json.load(last_downloaded_day_file)['last_downloaded_day'],
                "%Y-%m-%d"
            ).date()

        download_to.unlink(missing_ok=False)

        return last_downloaded_day

    def _update_last_downloaded_day(self, day):
        last_downloaded_day_dict = {"last_downloaded_day": day.strftime("%Y-%m-%d")}
        local_file = Path(self._workdir).joinpath(self._last_downloaded_day_filename)
        local_file.touch(exist_ok=True)

        with open(local_file, "w") as last_downloaded_day_file:
            json.dump(last_downloaded_day_dict, last_downloaded_day_file)

        self._s3_connector.upload_file(str(local_file), self._last_downloaded_day_filename)

        local_file.unlink(missing_ok=False)

    def _create_array_of_downloadable_days(self, date_from, date_to):
        downloadable_days = []

        while date_from < date_to:
            date_from = date_from + datetime.timedelta(days=1)
            downloadable_days.append(date_from)

        return downloadable_days

    def _get_downloadable_days(self):
        should_be_checked_since = datetime.datetime.utcnow().date() - datetime.timedelta(weeks=4)
        last_downloaded_day = self._get_last_downloaded_day()

        if last_downloaded_day < should_be_checked_since:
            date_from = last_downloaded_day
        else:
            date_from = should_be_checked_since

        downloadable_days = self._create_array_of_downloadable_days(date_from, datetime.datetime.utcnow().date())

        return downloadable_days

    def _save_to_s3(self, downloaded_file):
        self._s3_connector.upload_file(
            downloaded_file['downloaded_file_path'],
            downloaded_file['s3_bucket_key']
        )

        self._s3_connector.upload_file(
            downloaded_file['metadata_xml_file_path'],
            downloaded_file['metadata_xml_s3_bucket_key']
        )

        self._s3_connector.upload_file(
            downloaded_file['metadata_txt_file_path'],
            downloaded_file['metadata_txt_s3_bucket_key']
        )

        self._s3_connector.upload_file(
            downloaded_file['feature_id_file_path'],
            downloaded_file['feature_id_s3_bucket_key']
        )

        self._s3_connector.upload_file(
            downloaded_file['feature_json_file_path'],
            downloaded_file['feature_json_s3_bucket_key']
        )

        Path(downloaded_file['downloaded_file_path']).unlink(missing_ok=False)
        Path(downloaded_file['metadata_xml_file_path']).unlink(missing_ok=False)
        Path(downloaded_file['metadata_txt_file_path']).unlink(missing_ok=False)
        Path(downloaded_file['feature_id_file_path']).unlink(missing_ok=False)
        Path(downloaded_file['feature_json_file_path']).unlink(missing_ok=False)

        return downloaded_file

    def get_bbox(self, geojson):
        from geojson.utils import coords
        from shapely.geometry import LineString

        return list(LineString(coords(geojson)).bounds)

    def __catalogue_file(self, downloaded_file, geojson):
        from stac_templates.feature import feature

        feature['features'][0]['id'] = downloaded_file['displayId']
        feature['features'][0]['geometry'] = geojson
        feature['features'][0]['bbox'] = self.get_bbox(geojson)
        feature['features'][0]['properties']['datetime'] = downloaded_file['start'].isoformat()
        feature['features'][0]['properties']['start_datetime'] = downloaded_file['start'].isoformat()
        feature['features'][0]['properties']['end_datetime'] = downloaded_file['end'].isoformat()

        feature['features'][0]['assets'].update(
            {
                'title': downloaded_file['displayId'],
                'entity_id': downloaded_file['entityId'],
                'product_id': downloaded_file['productId'],
                'metadata': {
                    'txt': {
                        'href': 'http://147.251.115.146:8081/' + downloaded_file['metadata_txt_s3_bucket_key'],  # TODO
                        'type': mimetypes.guess_type(downloaded_file['metadata_txt_file_path'])[0]
                    },
                    'xml': {
                        'href': 'http://147.251.115.146:8081/' + downloaded_file['metadata_xml_s3_bucket_key'],  # TODO
                        'type': mimetypes.guess_type(downloaded_file['metadata_xml_file_path'])[0]
                    }
                },
                'data': {
                    'href': 'http://147.251.115.146:8081/' + downloaded_file['s3_bucket_key'],  # TODO
                    'type': mimetypes.guess_type(downloaded_file['filename'])[0]
                }
            }
        )

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

        return downloaded_file

    def _download_and_catalogize(self, downloadable_urls, geojson):
        for downloaded_file in downloadable_urls:
            # TODO pro každej soubor udělat vlákno

            """
            while True:
                try:
                    downloaded_file = self._download_file(downloaded_file)
                    break

                except LandsatDownloaderDownloadedFileHasDifferentSize as e:
                    self._logger.error(e)
                    self._logger.error("Redownloading...")
                    continue

            if downloaded_file is False:
                # File has already been downloaded, let's continue with the next file.
                continue
            """

            # TODO - napsat rozbalení metadat z taru
            downloaded_file.update(
                {
                    "metadata_xml_s3_bucket_key": f"{downloaded_file['dataset']}/{downloaded_file['displayId']}_MTL.xml",
                    "metadata_xml_file_path": str(
                        Path.joinpath(Path(self._workdir), downloaded_file['displayId'] + "_MTL.xml")),
                    "metadata_txt_s3_bucket_key": f"{downloaded_file['dataset']}/{downloaded_file['displayId']}_MTL.txt",
                    "metadata_txt_file_path": str(
                        Path.joinpath(Path(self._workdir), downloaded_file['displayId'] + "_MTL.txt")),
                })

            with tarfile.open(name=downloaded_file['downloaded_file_path']) as tar:
                try:
                    tar.extract(downloaded_file['displayId'] + "_MTL.txt", self._workdir)
                    tar.extract(downloaded_file['displayId'] + "_MTL.xml", self._workdir)
                except KeyError:
                    print(f"Warning: File not found in the tar archive.")

            downloaded_file = self.__catalogue_file(downloaded_file, geojson)
            downloaded_file = self._save_to_s3(downloaded_file)

        return True

    def run(self):
        days_to_download = self._get_downloadable_days()

        geojsons = {}
        geojson_files_paths = [Path(geojson_file) for geojson_file in Path('geojson').glob("*")]
        for geojson_file_path in geojson_files_paths:
            with open(geojson_file_path, 'r') as geojson_file:
                geojsons.update({geojson_file_path: json.loads(geojson_file.read())})

        for day in days_to_download:
            for dataset in self._demanded_datasets:
                for geojson_key in geojsons.keys():
                    self._logger.info(
                        f"Request for download dataset: {dataset}, location: {geojson_key}, " +
                        f"date_start: {day}, date_end: {day}."
                    )

                    scene_label = "landsat_downloader_"
                    downloadable_files_attributes = self._m2m_api_connector.get_downloadable_files(
                        dataset=dataset, geojson=geojsons[geojson_key], time_start=day, time_end=day, label=scene_label
                    )

                    downloaded_files = []
                    for downloadable_file_attributes in downloadable_files_attributes:
                        downloaded_files.append(
                            DownloadedFile(
                                attributes=downloadable_file_attributes,
                                stac_connector=self._stac_connector,
                                s3_connector=self._s3_connector,
                                workdir=self._workdir,
                                logger=self._logger
                            )
                        )

                    for downloaded_file in downloaded_files:
                        downloaded_file.process()  # TODO tady udělat threading

                    # self._download_and_catalogize(downloadable_files, geojsons[geojson_key])

                    self._m2m_api_connector.scene_list_remove(scene_label)

            self._update_last_downloaded_day(day)
