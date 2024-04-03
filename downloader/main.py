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


def exception_wait(sleep_time=(60 * 60)):
    """
    Function waits for specified time period (seconds)
    Logs critical since this function is meant to be called only when exception is raised

    :param sleep_time: int, seconds, default 60*60
    :return: nothing
    """

    logger.critical(f"Program will wait for {sleep_time} seconds.")
    time.sleep(sleep_time)


def setup_logging(current_path):
    """
    Setup logging and logrotating

    :param current_path: root dir of the script (./ of main.py)
    :return: nothing
    """

    log_dir = os.path.join(current_path, landsat_config.log_directory)
    log_file = os.path.join(str(log_dir), landsat_config.log_name)

    Path(str(log_dir)).mkdir(parents=True, exist_ok=True)

    logger_landsat = logging.getLogger(landsat_config.log_logger)
    logger_landsat.setLevel(landsat_config.log_level)

    log_format = logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s")

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

    try:
        logger.info("=== LANDSAT DOWNLOADER STARTING ===")

        landsat_downloader = None
        while landsat_downloader is None:
            try:
                # Initializing instance of LandsatDownloader, passing root and
                landsat_downloader = LandsatDownloader(
                    root_directory=root_dir,
                    logger=logger
                )
            except Exception as e:
                logger.critical(e, exc_info=True)
                exception_wait()
                continue

        logger.info("=== LANDSAT DOWNLOADER STARTED ===")

        while True:  # The downloading itself, repeat indefinitely
            next_run_at = datetime.datetime.combine(
                datetime.datetime.now(datetime.UTC).date() + datetime.timedelta(days=1),
                datetime.time(hour=9, minute=00)
            )  # Next run tommorow at 9am UTC

            try:
                landsat_downloader.run()

            except Exception as e:
                logger.critical(e, exc_info=True)
                exception_wait()
                continue

            now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            next_run_at = next_run_at.replace(tzinfo=None)
            while now < next_run_at:  # If now is not after scheduled run, we will wat for it
                sleep_for = int((next_run_at - now).total_seconds()) + 60
                logger.info(
                    f"All downloaded. Downloader will now wait for {str(sleep_for)} seconds. " +
                    f"Next run is scheduled to {str(next_run_at)}."
                )
                time.sleep(sleep_for)
                now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)

    except Exception as exception:
        logger.critical(exception)
        exit(-1)
