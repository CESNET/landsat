import json
import logging
import datetime
import os
import re
from pathlib import Path

import requests

import config.m2m_config as m2m_config

from m2m_api_connector import M2MAPIConnector
from stac_connector import STACConnector
from s3_connector import S3Connector


class LandsatDownloaderError(Exception):
    def __init__(self, message="Landsat Downloader General Error!"):
        self.message = message
        super().__init__(self.message)


class LandsatDownloaderUrlDoNotContainsFilename(Exception):
    def __init__(self, message="URL does not return filename!", url=None):
        if url is not None:
            self.message = message + " " + str(url)
        else:
            self.message = message

        super().__init__(self.message)


class LandsatDownloaderDownloadedFileHasDifferentSize(Exception):
    def __init__(
            self, message="Downloaded file size not matching expected file size!",
            content_length=None, file_size=None
    ):
        self.message = message + " Content-length: " + str(content_length) + ", file size: " + str(file_size) + "."
        super().__init__(self.message)


class LandsatDownloader:
    """
    __demanded_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]
    """
    _demanded_datasets = [
        "landsat_ot_c2_l1"
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
            logger=logging.getLogger('Downloader')
    ):
        logger.info("=== DOWNLOADER INITIALIZING ===")

        if root_directory is None:
            raise Exception("root_directory must be specified")

        if working_directory is None:
            raise Exception("working_directory must be specified")

        self.root_directory = root_directory
        self.workdir = str(os.path.join(self.root_directory, working_directory))
        self.logger = logger

        self._clean_up()

        self.m2m_api_connector = M2MAPIConnector(logger=self.logger)
        self.stac_connector = STACConnector(logger=logger)
        self.s3_connector = S3Connector(logger=logger)

        self.logger.info('=== DOWNLOADER INITIALIZED ===')

    def _clean_up(self):
        """
        Method initializes filesystem structure. Deleting old temp directories and creating new working directory
        specified in folder self.working_directory.

        :return: nothing
        """

        from shutil import rmtree

        pycache_dir = os.path.join(self.root_directory, "__pycache__")
        self.logger.info("Initial cleanup: Deleting " + pycache_dir)
        rmtree(pycache_dir, ignore_errors=True)

        self.logger.info("Initial cleanup: Deleting " + self.workdir)
        # rmtree(self.workdir, ignore_errors=True) # TODO workdir se má mazat, ale teď tam mám dočasně poslední aktualizaci

        self.logger.info("Initial cleanup: Creating directory " + self.workdir)
        Path(self.workdir).mkdir(parents=True, exist_ok=True)

    def _get_last_downloaded_day(self):  # TODO rewrite for S3 storage
        last_downloaded_day_file = open('workdir/last_downloaded_day.json')
        last_downloaded_day = datetime.datetime.strptime(
            json.load(last_downloaded_day_file)['last_downloaded_day'],
            "%Y-%m-%d"
        ).date()
        last_downloaded_day_file.close()

        return last_downloaded_day

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

    def _check_if_file_exists(self, downloaded_file, expected_size):
        # if the file exists, return True
        return self.s3_connector.check_if_key_exists(downloaded_file['s3_bucket_key'], expected_size)

    def _save_to_s3(self, downloaded_file):
        self.s3_connector.upload_file(
            downloaded_file['downloaded_file_path'],
            downloaded_file['s3_bucket_key']
        )

        Path(downloaded_file['downloaded_file_path']).unlink(missing_ok=False)

        return downloaded_file

    def __catalogize_file(self, downloaded_file):

        pass

    def _download_file(self, downloaded_file):
        response = requests.get(downloaded_file['url'], stream=True)

        Path(self.workdir).mkdir(exist_ok=True)

        filename = None

        for content_disposition in response.headers['Content-Disposition'].split(' '):
            if 'filename' in content_disposition:
                filename = re.findall(r'"([^"]*)"', content_disposition)[0]

        if filename is None:
            raise LandsatDownloaderUrlDoNotContainsFilename(url=downloaded_file['url'])

        downloaded_file.update(
            {
                "filename": filename,
                "s3_bucket_key": f"{downloaded_file['dataset']}/{filename}"
            }
        )

        if self._check_if_file_exists(downloaded_file, response.headers['Content-Length']):
            # Well the file has already been downloaded, so there is no need to download it again and this
            # method cannot succeed in downloading the file that has already been downloaded. Let's return False.
            self.logger.info(f"File {downloaded_file['s3_bucket_key']} has been already downloaded. Skipping.")
            return False

        downloaded_file_path = os.path.join(self.workdir, filename)
        Path(downloaded_file_path).touch()

        self.logger.info("Downloading " + downloaded_file['url'] + " into " + downloaded_file_path + ".")

        with open(downloaded_file_path, mode='wb') as result_file:
            for chunk in response.iter_content(chunk_size=(1024 * 1024)):
                result_file.write(chunk)

        content_length = int(response.headers['Content-Length'])
        file_size = os.stat(downloaded_file_path).st_size
        if content_length != file_size:
            raise LandsatDownloaderDownloadedFileHasDifferentSize(content_length=content_length, file_size=file_size)

        downloaded_file.update({"downloaded_file_path": downloaded_file_path})

        return downloaded_file

    def _download_and_catalogize(self, downloadable_urls):
        for downloaded_file in downloadable_urls:

            downloaded_file = self._download_file(downloaded_file)
            if downloaded_file is False:
                # File has already been downloaded, let's continue with next file.
                continue

            downloaded_file = self._save_to_s3(downloaded_file)
            self.__catalogize_file(downloaded_file)

        return True

    def run(self):
        days_to_download = self._get_downloadable_days()

        geojsons = {}
        geojson_files_paths = [os.path.join('geojson', geojson_file) for geojson_file in os.listdir('geojson')]
        for geojson_file_path in geojson_files_paths:
            with open(geojson_file_path, 'r') as geojson_file:
                geojsons.update({geojson_file_path: json.loads(geojson_file.read())})

        for day in days_to_download:
            for dataset in self._demanded_datasets:
                for geojson_key in geojsons.keys():
                    self.logger.info(
                        "Request for download dataset: {}, location: {}, date_start: {}, date_end: {}.".format(
                            dataset, geojson_key, day, day
                        )
                    )

                    label = "landsat_downloader_"
                    downloadable_urls = self.m2m_api_connector.get_downloadable_urls(
                        dataset=dataset, geojson=geojsons[geojson_key], time_start=day, time_end=day, label=label
                    )

                    self._download_and_catalogize(downloadable_urls)

                    self.m2m_api_connector.scene_list_remove(label)
