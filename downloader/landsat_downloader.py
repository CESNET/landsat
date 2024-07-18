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


class LandsatDownloader:
    """
    Class representing the Landsat downloader package
    """

    """
    Filename of a .json file which contains information of last day that was downloaded.
    By default located in s3 bucket landsat/last_downloaded_day.json
    """
    _last_downloaded_day_filename = 'last_downloaded_day.json'

    def __init__(
            self,
            demanded_datasets=landsat_config.demanded_datasets,
            m2m_username=None, m2m_token=None,
            stac_username=None, stac_password=None,
            s3_endpoint=None, s3_access_key=None, s3_secret_key=None, s3_host_bucket=None,
            root_directory: Path = None, working_directory: Path = Path(landsat_config.working_directory),
            logger=logging.getLogger('LandsatDownloader'),
            feature_download_host=landsat_config.s3_download_host
    ):
        """
        __init__
        :param demanded_datasets: Datasets demanded to download
        :param m2m_username: Username used to log into USGS M2M API
        :param m2m_token: Login token for USGS M2M API (Generated here: https://ers.cr.usgs.gov/)
        :param stac_username: Username used for publishing features into STAC API
        :param stac_password: Password of STAC API
        :param s3_endpoint: URL of a S3 into which the downloaded data is registered
        :param s3_access_key: Access key for S3 bucket
        :param s3_secret_key: Secret key for S3 bucket
        :param s3_host_bucket: S3 host bucket (by default it should be "landsat")
        :param root_directory: Absolute path to the root directory of the script
        :param working_directory: Absolute path to the working directory (temporary dir for downloaded data etc.)
        :param logger:
        :param feature_download_host: URL of a host on which the http_server script is running
        """
        logger.debug("=== DOWNLOADER INITIALIZING ===")

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

        """
        Datasets which will be downloaded
        """
        self._demanded_datasets = demanded_datasets

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
        self._logger.debug(f"Initial cleanup: Deleting {self._workdir}")
        # self._workdir.unlink(missing_ok=True) # Does not work, returns WinError 5: Access Denied
        shutil.rmtree(self._workdir, ignore_errors=True)

        self._logger.debug(f"Initial cleanup: Creating directory {self._workdir}")
        self._workdir.mkdir(parents=True, exist_ok=True)

    def _get_last_downloaded_day(self):
        """
        Method reads date of last downloaded day from S3 storage
        :return: datetime of last downloaded day
        """

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
        """
        Method updates the file with date of last downloaded day on S3

        :param day: datetime of last downloaded day
        :return: nothing
        """

        if self._last_downloaded_day > day:
            return

        last_downloaded_day_dict = {"last_downloaded_day": day.strftime("%Y-%m-%d")}
        local_file = Path(self._workdir).joinpath(self._last_downloaded_day_filename)
        local_file.touch(exist_ok=True)

        with open(local_file, "w") as last_downloaded_day_file:
            json.dump(last_downloaded_day_dict, last_downloaded_day_file)

        self._s3_connector.upload_file(str(local_file), self._last_downloaded_day_filename)

        local_file.unlink(missing_ok=False)

    def _create_array_of_downloadable_days(self, date_from, date_to):
        """
        Method creates an array of days which are meant to be downloaded by input parameters

        :param date_from: datetime of first day meant to be downloaded
        :param date_to: datetime of last day meant to be downloaded
        :return: array of datetimes
        """

        downloadable_days = []

        while date_from < date_to:
            date_from = date_from + datetime.timedelta(days=1)
            downloadable_days.append(date_from)

        return downloadable_days

    def _get_downloadable_days(self):
        """
        Method creates a date range from the day which must be downloaded first to the day which must be downloaded
        last. Then using method _create_array_of_downloadable_days() method generates an array of all days which are
        meant to be downloaded.
        Everytime at least four weeks are being downloaded.

        :return: array of datetime
        """

        should_be_checked_since = datetime.datetime.utcnow().date() - datetime.timedelta(weeks=4)
        self._last_downloaded_day = self._get_last_downloaded_day()

        if self._last_downloaded_day < should_be_checked_since:
            date_from = self._last_downloaded_day
        else:
            date_from = should_be_checked_since

        downloadable_days = self._create_array_of_downloadable_days(date_from, datetime.datetime.utcnow().date())

        return downloadable_days

    def run(self):
        """
        Main worker of LandsatDownloader class.
        Method prepares array of days which are in need of downloaading and dictionary of geojsons for which we
        are downloading files. These geojsons must be saved in ./geojson directory.
        Then for every demanded day, dataset and geojson this method prepares M2M API scenes and retrieves URLs of
        available dataset.
        For every URL is created standalone instance of DownloadedFile class in which the method process() is executed
        in threads.
        When all the data for one of the demanded days is downloaded, this method invokes _update_last_downloaded_day()
        and updates the last downloaded day accordingly.

        :return: nothing
        """

        days_to_download = self._get_downloadable_days()

        """
        Preparing the dict of demanded geojsons
        """
        geojsons = {}
        geojson_files_paths = [Path(geojson_file) for geojson_file in Path('geojson').glob("*")]
        for geojson_file_path in geojson_files_paths:
            with open(geojson_file_path, 'r') as geojson_file:
                geojsons.update({geojson_file_path: json.loads(geojson_file.read())})

        for day in days_to_download:  # For each demanded day...
            for dataset in self._demanded_datasets:  # ...each demanded dataset...
                for geojson_key in geojsons.keys():  # ...and each demanded geojson...
                    self._logger.info(
                        f"Request for download dataset: {dataset}, location: {geojson_key}, " +
                        f"date_start: {day}, date_end: {day}."
                    )

                    scene_label = landsat_config.m2m_scene_label
                    downloadable_files_attributes = self._m2m_api_connector.get_downloadable_files(
                        dataset=dataset, geojson=geojsons[geojson_key], time_start=day, time_end=day, label=scene_label
                    )

                    thread_lock = threading.Lock()
                    downloaded_files = []
                    for downloadable_file_attributes in downloadable_files_attributes:
                        downloaded_files.append(
                            DownloadedFile(
                                attributes=downloadable_file_attributes,
                                stac_connector=self._stac_connector,
                                s3_connector=self._s3_connector,
                                workdir=self._workdir,
                                logger=self._logger,
                                thread_lock=thread_lock
                            )
                        )

                    threads = []  # Into this list we will save all the threads that we will run

                    for downloaded_file in downloaded_files:
                        threads.append(
                            threading.Thread(
                                target=downloaded_file.process,
                                name=f"Thread-{downloaded_file.get_display_id()}"
                            )
                        )  # Preparing threads to be executed

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

                    for downloaded_file in downloaded_files:
                        if downloaded_file.exception_occurred is not None:
                            raise downloaded_file.exception_occurred

            self._update_last_downloaded_day(day)
