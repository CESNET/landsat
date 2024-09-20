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

m2m_scene_label = "landsat_downloader_testing"

# When changing port it must be reflected in http_server/docker-compose.yml!
s3_download_host = "http://chronos.dhr.cesnet.cz:8081/"

demanded_datasets = [
    "landsat_ot_c2_l1", "landsat_ot_c2_l2",
    "landsat_etm_c2_l1", "landsat_etm_c2_l2",
    "landsat_tm_c2_l1", "landsat_tm_c2_l2",
    "landsat_mss_c2_l1"
]

"""
catalogue_only variable specifies whether we want to download file from USGS M2M API.
True: script will download only files that has not been downloaded from USGS M2M API,
    script won't re-download already downloaded data for example if the size has changed.
False: script will check every file (even downloaded ones) against USGS M2M API, 
    and will redownload all of them for example if size has changed
"""
catalogue_only = False

"""
force_redownload_file variable specifies whether we want to download file everytime 
True: if the file has already been downloaded, we will overwrite it anyway
False: the new file will be downloaded only if new file differs for example in size
"""
force_redownload_file = False
