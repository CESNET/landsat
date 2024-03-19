import datetime
import re
import sys
import time
import os
import logging

from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

import config.landsat_config as landsat_config

from landsat_downloader import LandsatDownloader

from exceptions.m2m_api_connector import *
from exceptions.stac_connector import *


def setup_logging(current_path):
    log_dir = os.path.join(current_path, landsat_config.log_directory)
    log_file = os.path.join(str(log_dir), landsat_config.log_name)

    Path(str(log_dir)).mkdir(parents=True, exist_ok=True)

    logger_landsat = logging.getLogger(landsat_config.log_logger)
    logger_landsat.setLevel(landsat_config.log_level)

    log_format = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-4.4s]  %(message)s")

    rotating_info_handler = TimedRotatingFileHandler(log_file, when="midnight")
    rotating_info_handler.setFormatter(log_format)
    rotating_info_handler.setLevel(landsat_config.log_level)
    rotating_info_handler.suffix = "%Y%m%d%H%M%S"
    rotating_info_handler.extMatch = re.compile(r"^\d{14}$")
    logger_landsat.addHandler(rotating_info_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(landsat_config.log_level)
    stdout_handler.setFormatter(log_format)
    logger_landsat.addHandler(stdout_handler)


if __name__ == '__main__':
    logger = logging.getLogger(landsat_config.log_logger)
    root_dir = Path(__file__).parent.resolve()

    setup_logging(root_dir)
    logger.info("=== LANDSAT DOWNLOADER STARTING ===")

    landsat_downloader = LandsatDownloader(
        root_directory=root_dir,
        working_directory=Path(landsat_config.working_directory),
        logger=logger,
        feature_download_host=landsat_config.s3_download_host
    )
    logger.info("=== LANDSAT DOWNLOADER STARTED ===")

    while True:
        start_time = datetime.datetime.utcnow()
        next_run_at = datetime.datetime.combine(
            datetime.datetime.utcnow().date() + datetime.timedelta(days=1),
            datetime.time(hour=9, minute=00)
        )

        exception_occurred = False
        try:
            landsat_downloader.run()

        except M2MAPIRequestTimeout as e:
            logger.critical(e, exc_info=True)

            sleep_for = 60 * 60
            logger.critical(f"Program will wait for {sleep_for} seconds.")
            time.sleep(sleep_for)

            continue

        except STACRequestTimeout as e:
            logger.critical(e, exc_info=True)

            sleep_for = 60 * 60
            logger.critical(f"Program will wait for {sleep_for} seconds.")
            time.sleep(sleep_for)

            continue

        except Exception as e:
            exception_occurred = True
            logger.critical(e, exc_info=True)
            exit(-1)

        while datetime.datetime.utcnow() < next_run_at:
            sleep_for = int((next_run_at - datetime.datetime.utcnow()).total_seconds())
            logger.info(
                "Downloader will wait " + str(sleep_for) + " seconds. Next run will be at " + str(next_run_at))
            time.sleep(sleep_for)
