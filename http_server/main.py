# aws must be set: https://du.cesnet.cz/cs/navody/object_storage/awscli/start

from pathlib import Path
import logging
import sys

from http.server import BaseHTTPRequestHandler, HTTPServer
import os

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

host_name = "0.0.0.0"  # Listen everywhere
server_port = 8080


def setup_logging(current_path):
    log_dir = current_path + '/' + log_directory
    log_file = log_dir + '/' + log_name

    os.makedirs(log_dir, exist_ok=True)
    Path(log_file).touch()

    logging.basicConfig(
        filename=log_file,
        encoding='utf-8',
        level=log_level,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        force=True
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class Server(BaseHTTPRequestHandler):
    def do_GET(self):
        s3_url = "s3://landsat" + self.path  # Prepares path to S3 bucket using requested URL

        aws_command = "aws s3 --endpoint-url https://s3.cl4.du.cesnet.cz presign " + s3_url
        s3_download_url = os.popen(aws_command).read()  # Generate temporary link to download file from S3 bucket

        # Redirecting to temporary S3 download link
        self.send_response(301)
        self.send_header('Location', s3_download_url)
        self.end_headers()


if __name__ == "__main__":
    setup_logging(str(Path(__file__).parent.resolve()))

    webServer = HTTPServer((host_name, server_port), Server)
    print("Server started http://%s:%s" % (host_name, server_port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
