import os

SANIC__APP_NAME = os.environ.get("SANIC__APP_NAME", default="landsat_http_server")
SANIC__SERVER_HOST = os.environ.get("SANIC__SERVER_HOST", default="0.0.0.0")
SANIC__SERVER_PORT = int(os.environ.get("SANIC__SERVER_PORT", default=8080))

S3_CONNECTOR__HOST_BASE = os.getenv("S3_CONNECTOR__HOST_BASE")
S3_CONNECTOR__HOST_BUCKET = os.getenv("S3_CONNECTOR__HOST_BUCKET")
S3_CONNECTOR__CREDENTIALS = {
    'access_key': os.getenv("S3_CONNECTOR__ACCESS_KEY"),
    'secret_key': os.getenv("S3_CONNECTOR__SECRET_KEY"),
}

LOGGER__NAME = os.getenv("LOGGER__NAME", default="landsat_http_server")
LOGGER__LOG_DIRECTORY = os.getenv("LOGGER__LOG_DIRECTORY", default="./")
LOGGER__LOG_FILENAME = os.getenv("LOGGER__LOG_FILENAME", default="landsat_http_server.log")
LOGGER__LOG_LEVEL = int(os.getenv("LOGGER__LOG_LEVEL", default=20))
