import json
import logging
import datetime
import threading

from pathlib import Path

from m2m_api_connector import M2MAPIConnector
from stac_connector import STACConnector
from s3_connector import S3Connector
from downloaded_file import DownloadedFile

from config import landsat_config

"""
Create file ./config/landsat_config.py with following content:

log_directory = "log"
log_name = "landsat.log"
log_level = 20
log_logger = "LandsatLogger"
working_directory = "workdir"
s3_download_host = "host with s3 download relay"

...or your values could be different. As you wish.
"""


class LandsatDownloader:
    _last_downloaded_day_filename = 'last_downloaded_day.json'

    _demanded_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]

    def __init__(
            self,
            m2m_username=None, m2m_token=None,
            stac_username=None, stac_password=None,
            s3_endpoint=None, s3_access_key=None, s3_secret_key=None, s3_host_bucket=None,
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

        if (m2m_username is None) or (m2m_token is None):
            self._m2m_api_connector = M2MAPIConnector(logger=self._logger)
        else:
            self._m2m_api_connector = M2MAPIConnector(username=m2m_username, token=m2m_token, logger=self._logger)

        if (stac_username is None) or (stac_password is None):
            self._stac_connector = STACConnector(logger=logger)
        else:
            self._stac_connector = STACConnector(username=stac_username, password=stac_password, logger=logger)

        if (s3_endpoint is None) or (s3_access_key is None) or (s3_secret_key is None) or (s3_host_bucket is None):
            self._s3_connector = S3Connector(logger=logger)
        else:
            self._s3_connector = S3Connector(
                s3_endpoint=s3_endpoint,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
                host_bucket=s3_host_bucket,
                logger=logger
            )

        self._logger.info('=== DOWNLOADER INITIALIZED ===')

    def _clean_up(self):
        """
        Method initializes filesystem structure. Deleting old temp directories and creating new working directory
        specified in folder self.working_directory.

        :return: nothing
        """

        """
        pycache_dir = self._root_directory.joinpath("__pycache__")
        self._logger.info(f"Initial cleanup: Deleting {pycache_dir}")
        pycache_dir.unlink(missing_ok=True)
        """

        import shutil
        self._logger.info(f"Initial cleanup: Deleting {self._workdir}")
        # self._workdir.unlink(missing_ok=True) # Does not work, returns WinError 5: Access Denied
        shutil.rmtree(self._workdir, ignore_errors=True)

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

                    scene_label = landsat_config.scene_label
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

                    threads = []

                    for downloaded_file in downloaded_files:
                        threads.append(
                            threading.Thread(
                                target=downloaded_file.process,
                                name=f"Thread-{downloaded_file.get_display_id()}"
                            )
                        )

                    started_threads = []  # There are no started threads
                    for thread in threads:
                        if len(started_threads) < 10:  # If there is less than 10 started threads...
                            thread.start()  # ...start a new one...
                            started_threads.append(thread)  # ...and add it into the list of started threads

                        else:  # If there is 10 or more started threads, we can not start a new thread...
                            for started_thread in started_threads:
                                started_thread.join()  # ...so we wait for those started threads to finish...

                            started_threads = []  # ...then we clear the array of started threads...

                            thread.start()  # ...start the thread we wanted to start as eleventh thread...
                            started_threads.append(thread)  # ...and add it to the list of started threads.

                    for thread in threads:
                        thread.join()  # In the end we will wait for all the threads to finish

                    self._m2m_api_connector.scene_list_remove(scene_label)

            self._update_last_downloaded_day(day)
