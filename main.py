import datetime
import re
import sys
import time
from logging.handlers import TimedRotatingFileHandler

import config.config as config

from Downloader import *


def setup_logging(current_path):
    log_dir = current_path + '/' + config.log_directory
    log_file = log_dir + '/' + config.log_name

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger_landsat = logging.getLogger(config.log_logger)
    logger_landsat.setLevel(config.log_level)

    log_format = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

    rotating_info_handler = TimedRotatingFileHandler(log_file, when="midnight")
    rotating_info_handler.setFormatter(log_format)
    rotating_info_handler.setLevel(config.log_level)
    rotating_info_handler.suffix = "%Y%m%d%H%M%S"
    rotating_info_handler.extMatch = re.compile(r"^\d{14}$")
    logger_landsat.addHandler(rotating_info_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(config.log_level)
    stdout_handler.setFormatter(log_format)
    logger_landsat.addHandler(stdout_handler)

if __name__ == '__main__':
    logger = logging.getLogger(config.log_logger)

    root_dir = str(Path(__file__).parent.resolve())

    setup_logging(root_dir)
    logger.info("=== LANDSAT DOWNLOADER STARTING ===")

    downloader = Downloader(root_dir=root_dir, logger=logger)
    logger.info("=== LANDSAT DOWNLOADER STARTED ===")

    while True:
        start_time = datetime.datetime.utcnow()
        next_run_at = datetime.datetime.combine(
            datetime.datetime.utcnow().date() + datetime.timedelta(days=1),
            datetime.time(hour=9, minute=00)
        )

        downloader.run()

        while datetime.datetime.utcnow() < next_run_at:
            sleep_for = int((next_run_at - datetime.datetime.utcnow()).total_seconds())
            logger.info("Downloader will wait " + str(sleep_for) + " seconds. Next run will be at " + str(next_run_at))
            time.sleep(sleep_for)
