import json
import logging
import datetime
from pathlib import Path

from m2m_api.api import M2M
from config import m2m_credentials


class ApiConnector:
    allowed_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]

    dataset_fullname = {
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
            username=None,
            token=None,
            root_dir=None,
            logger=logging.getLogger('logger_apiConnector')
    ):
        if root_dir is None:
            raise Exception("root_dir must be specified")

        self.root_dir = root_dir
        self.logger = logger
        self.workdir = root_dir + 'workdir'

        if username is not None:
            self.username = username
        else:
            self.username = m2m_credentials.username

        if token is not None:
            self.token = token
        else:
            self.token = m2m_credentials.token

        self.m2m = M2M(username=self.username, token=self.token)

    def __get_last_downloaded_day(self):
        last_downloaded_day_file = open('workdir/last_downloaded_day.json')
        last_downloaded_day = datetime.datetime.strptime(
            json.load(last_downloaded_day_file)['last_downloaded_day'],
            "%Y-%m-%d"
        ).date()
        last_downloaded_day_file.close()

        return last_downloaded_day

    def __should_download_new_data(self, last_downloaded, yesterday):
        if yesterday > last_downloaded:
            self.logger.info(
                "Data were last downloaded on: " + str(last_downloaded) +
                ", yesterday was: " + str(yesterday) +
                " and so new data must be downloaded."
            )
            return True

        else:
            self.logger.info(
                "Data were last downloaded on: " + str(last_downloaded) +
                ", yesterday was: " + str(yesterday) +
                " and so there is no need to download the data again. I'll try again tomorrow."
            )
            return False

    def __create_array_of_downloadable_days(self, last_downloaded_day, yesterday):
        downloadable_days = []

        while last_downloaded_day < yesterday:
            last_downloaded_day = last_downloaded_day + datetime.timedelta(days=1)
            downloadable_days.append(last_downloaded_day)

        return downloadable_days

    def init(self):
        """
        Method initializes filesystem structure. Deleting old temp directories and creating new working directory
        specified in folder self.working_directory.

        :return: nothing
        """

        from shutil import rmtree

        self.logger.info("Initial cleanup: Deleting " + self.root_dir + "/__pycache__")
        rmtree(self.root_dir + "/__pycache__", ignore_errors=True)

        self.logger.info("Initial cleanup: Deleting " + self.workdir)
        # rmtree(self.workdir, ignore_errors=True) # TODO workdir se má mazat, ale teď tam mám dočasně poslední aktualizaci

        self.logger.info("Initial cleanup: Creating directory " + self.workdir)
        Path(self.workdir).mkdir(parents=True, exist_ok=True)

    def run(self):
        self.init()

        self.logger.info("=== DOWNLOADER STARTED ===")

    def download_to_present(self):
        yesterday = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
        last_downloaded_day = self.__get_last_downloaded_day()

        if not self.__should_download_new_data(last_downloaded_day, yesterday):
            # Data are up-to-date until yesterday, hence there is no need to download next data.
            # We can return True, since everything is downloaded.
            return True

        downloadable_days = self.__create_array_of_downloadable_days(last_downloaded_day, yesterday)

        self.logger.info("Days that must be downloaded: " + str(downloadable_days))
