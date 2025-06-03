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



# When changing port it must be reflected in http_server/docker-compose.yml! # TODO not anymore - changed to apache2 proxy
# Ending slash is mandatory!
s3_download_host = "https://chronos.dhr.cesnet.cz/landsat/"
