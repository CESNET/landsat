# aws must be set: https://du.cesnet.cz/cs/navody/object_storage/awscli/start

import logging
import os
import re
import sys

from logging.handlers import TimedRotatingFileHandler

from pathlib import Path

from flask import Flask
from flask import redirect as flask_redirect

from waitress import serve

server_host = "0.0.0.0"  # Listen everywhere
server_port = 8080

log_logger = "HttpServerLogger"
log_directory = './log'
log_name = 'http-server.log'
log_level = 20
"""
LOG LEVELS:

CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0
"""


def setup_logging(current_path):
    if log_directory[0] == '/':
        log_dir = Path(log_directory)
    else:
        log_dir = os.path.join(current_path, log_directory)
    log_file = os.path.join(str(log_dir), log_name)

    Path(str(log_dir)).mkdir(parents=True, exist_ok=True)

    logger_http_server = logging.getLogger(log_logger)
    logger_http_server.setLevel(log_level)

    log_format = logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s")

    rotating_info_handler = TimedRotatingFileHandler(log_file, when="midnight")
    rotating_info_handler.setFormatter(log_format)
    rotating_info_handler.setLevel(log_level)
    rotating_info_handler.suffix = "%Y%m%d%H%M%S"
    rotating_info_handler.extMatch = re.compile(r"^\d{14}$")
    logger_http_server.addHandler(rotating_info_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)
    logger_http_server.addHandler(stdout_handler)

    return logger_http_server


logger = setup_logging(str(Path(__file__).parent.resolve()))
flask_app = Flask(__name__)


def bad_request(path="NOT_SPECIFIED"):
    logger.info(f"Got path: {path}, returning Bad Request (HTTP/400)!")
    return "Bad Request", 400


@flask_app.route("/")
def slash():
    return bad_request(path="/")


@flask_app.route('/<path:path>')
def redirect(path):
    if not ("landsat" in path):
        return bad_request(path=path)

    s3_url = f"s3://landsat/{path}"  # Prepares path to S3 bucket using requested URL

    aws_command = f"aws s3 --endpoint-url https://s3.cl4.du.cesnet.cz presign {s3_url}"
    s3_download_url = os.popen(aws_command).read().strip()  # Generate temporary link to download file from S3 bucket

    logger.info(f"Got path: {path}, redirecting to: " + s3_download_url)

    return flask_redirect(s3_download_url, code=302)


if __name__ == '__main__':
    serve(flask_app, host=server_host, port=server_port)
