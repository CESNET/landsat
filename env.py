import os

from dotenv import load_dotenv

from exceptions.env import *

load_dotenv()

true_statements = ["1", "true", "yes"]

LANDSAT__DEMANDED_DATASETS_POSSIBLE_OPTIONS: list = [
    "landsat_ot_c2_l1", "landsat_ot_c2_l2",
    "landsat_etm_c2_l1", "landsat_etm_c2_l2",
    "landsat_tm_c2_l1", "landsat_tm_c2_l2",
    "landsat_mss_c2_l1"
]
LANDSAT__DEMANDED_DATASETS: list = os.environ.get(
    "LANDSAT__DEMANDED_DATASETS", default="landsat_ot_c2_l1;landsat_ot_c2_l2"
).split(";")
if not all(item in LANDSAT__DEMANDED_DATASETS_POSSIBLE_OPTIONS for item in LANDSAT__DEMANDED_DATASETS):
    raise UnexpectedDataset(LANDSAT__DEMANDED_DATASETS)

LANDSAT__M2M_SCENE_LABEL: str = os.environ.get("LANDSAT__M2M_SCENE_LABEL", default="landsat_downloader_stable")

LANDSAT__CATALOGUE_ONLY = os.getenv("LANDSAT__CATALOGUE_ONLY", default="false").lower() in true_statements
LANDSAT__FORCE_REDOWNLOAD_FILE = os.getenv("LANDSAT__FORCE_REDOWNLOAD_FILE", default="false").lower() in true_statements


BASE__APP_NAME = os.environ.get("BASE__APP_NAME", default="dhr-download-stac-relay")

S3_CONNECTOR__LANDSAT = {
    'host_base': os.environ.get("S3_CONNECTOR__LANDSAT_HOST_BASE"),
    'host_bucket': os.environ.get("S3_CONNECTOR__LANDSAT_HOST_BUCKET"),
    'access_key': os.environ.get("S3_CONNECTOR__LANDSAT_ACCESS_KEY"),
    'secret_key': os.environ.get("S3_CONNECTOR__LANDSAT_SECRET_KEY")
}

LOGGER__NAME = os.environ.get("LOGGER__NAME", default=BASE__APP_NAME)
LOGGER__LOG_DIRECTORY = os.environ.get("LOGGER__LOG_DIRECTORY", default="./log")
LOGGER__LOG_FILENAME = os.environ.get("LOGGER__LOG_FILENAME", default=f"{BASE__APP_NAME}.log")
LOGGER__LOG_LEVEL = int(os.environ.get("LOGGER__LOG_LEVEL", default=20))
