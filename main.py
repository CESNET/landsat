import re
from logging.handlers import TimedRotatingFileHandler

import config.config as config

from api_connector import *


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


if __name__ == '__main__':
    root_dir = str(Path(__file__).parent.resolve())

    setup_logging(root_dir)
    logging.getLogger(config.log_logger).info("=== LANDSAT INITIALIZATION ===")

    api_connector = ApiConnector(root_dir=root_dir, logger=logging.getLogger(config.log_logger))
    api_connector.download_to_present()
