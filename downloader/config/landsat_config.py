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
log_directory = "log"
log_name = "landsat.log"
log_level = 20
log_logger = "LandsatLogger"

working_directory = "workdir"

m2m_scene_label = "landsat_downloader_"

### When changing port it must be reflected in http_server/docker-compose.yml!
s3_download_host = "http://chronos.dhr.cesnet.cz:8081/"

demanded_datasets = [
    "landsat_ot_c2_l1", "landsat_ot_c2_l2",
    "landsat_etm_c2_l1", "landsat_etm_c2_l2",
    "landsat_tm_c2_l1", "landsat_tm_c2_l2",
    "landsat_mss_c2_l1"
]
