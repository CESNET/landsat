import json
import logging
import datetime
import os
from pathlib import Path

import api_connector

from config import m2m_credentials


class Downloader:
    __demanded_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]

    __dataset_fullname = {
        "landsat_ot_c2_l1": "Landsat 8-9 OLI/TIRS C2 L1",
        "landsat_ot_c2_l2": "Landsat 8-9 OLI/TIRS C2 L2",
        "landsat_etm_c2_l1": "Landsat 7 ETM+ C2 L1",
        "landsat_etm_c2_l2": "Landsat 7 ETM+ C2 L2",
        "landsat_tm_c2_l1": "Landsat 4-5 TM C2 L1",
        "landsat_tm_c2_l2": "Landsat 4-5 TM C2 L2",
        "landsat_mss_c2_l1": "Landsat 1-5 MSS C2 L1"
    }

    def __init__(self, username=None, token=None, root_dir=None, logger=logging.getLogger('Downloader')):
        logger.info("=== DOWNLOADER INITIALIZING ===")

        if root_dir is None:
            raise Exception("root_dir must be specified")

        self.root_dir = root_dir
        self.logger = logger
        self.workdir = str(os.path.join(root_dir, 'workdir'))

        self.__clean_up()

        if username is not None:
            self.username = username
        else:
            self.username = m2m_credentials.username

        if token is not None:
            self.token = token
        else:
            self.token = m2m_credentials.token

        self.api_connector = api_connector.APIConnector(
            logger=self.logger,
            username=self.username,
            token=self.token
        )

        self.logger.info('=== DOWNLOADER INITIALIZED ===')

    def __clean_up(self):
        """
        Method initializes filesystem structure. Deleting old temp directories and creating new working directory
        specified in folder self.working_directory.

        :return: nothing
        """

        from shutil import rmtree

        pycache_dir = os.path.join(self.root_dir, "__pycache__")
        self.logger.info("Initial cleanup: Deleting " + pycache_dir)
        rmtree(pycache_dir, ignore_errors=True)

        self.logger.info("Initial cleanup: Deleting " + self.workdir)
        # rmtree(self.workdir, ignore_errors=True) # TODO workdir se má mazat, ale teď tam mám dočasně poslední aktualizaci

        self.logger.info("Initial cleanup: Creating directory " + self.workdir)
        Path(self.workdir).mkdir(parents=True, exist_ok=True)

    def run(self):
        days_to_download = self.__get_downloadable_days()

        for day in days_to_download:
            for dataset in self.__demanded_datasets:
                self.api_connector.download_dataset(dataset, day, day)

    def __get_last_downloaded_day(self):  # TODO rewrite for S3 storage
        last_downloaded_day_file = open('workdir/last_downloaded_day.json')
        last_downloaded_day = datetime.datetime.strptime(
            json.load(last_downloaded_day_file)['last_downloaded_day'],
            "%Y-%m-%d"
        ).date()
        last_downloaded_day_file.close()

        return last_downloaded_day

    def __create_array_of_downloadable_days(self, date_from, date_to):
        downloadable_days = []

        while date_from < date_to:
            date_from = date_from + datetime.timedelta(days=1)
            downloadable_days.append(date_from)

        return downloadable_days

    def __get_downloadable_days(self):
        should_be_checked_since = datetime.datetime.utcnow().date() - datetime.timedelta(weeks=4)
        last_downloaded_day = self.__get_last_downloaded_day()

        if last_downloaded_day < should_be_checked_since:
            date_from = last_downloaded_day
        else:
            date_from = should_be_checked_since

        downloadable_days = self.__create_array_of_downloadable_days(date_from, datetime.datetime.utcnow().date())

        return downloadable_days
