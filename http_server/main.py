import sys
import re
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from sanic_server import SanicServer

import config.variables as variables

def setup_logging(current_path):
    if variables.LOGGER__LOG_DIRECTORY[0] == '/':
        log_dir = Path(variables.LOGGER__LOG_DIRECTORY)
    else:
        log_dir = os.path.join(current_path, variables.LOGGER__LOG_DIRECTORY)
    log_file = os.path.join(str(log_dir), variables.LOGGER__LOG_FILENAME)

    Path(str(log_dir)).mkdir(parents=True, exist_ok=True)

    logger_http_server = logging.getLogger(variables.LOGGER__NAME)
    logger_http_server.setLevel(variables.LOGGER__LOG_LEVEL)

    log_format = logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s]: %(message)s")

    rotating_info_handler = TimedRotatingFileHandler(log_file, when="midnight")
    rotating_info_handler.setFormatter(log_format)
    rotating_info_handler.setLevel(variables.LOGGER__LOG_LEVEL)
    rotating_info_handler.suffix = "%Y%m%d%H%M%S"
    rotating_info_handler.extMatch = re.compile(r"^\d{14}$")
    logger_http_server.addHandler(rotating_info_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(variables.LOGGER__LOG_LEVEL)
    stdout_handler.setFormatter(log_format)
    logger_http_server.addHandler(stdout_handler)

    return logger_http_server


logger = setup_logging(str(Path(__file__).parent.resolve()))

server = SanicServer(logger=logger)
app = server.get_app()

if __name__ == "__main__":
    server.run()
