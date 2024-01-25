import logging
import os

import config.m2m_credentials as m2m_credentials


class ApiConnector:
    allowed_datasets = [
        "landsat_ot_c2_l1", "landsat_ot_c2_l2",
        "landsat_etm_c2_l1", "landsat_etm_c2_l2",
        "landsat_tm_c2_l1", "landsat_tm_c2_l2",
        "landsat_mss_c2_l1"
    ]

    dataset_fullname = {
        "landsat_ot_c2_l1": "Landsat 8-9 OLI/TIRS C2 L1",
        "landsat_ot_c2_l2": "Landsat 8-9 OLI/TIRS C2 L2",
        "landsat_etm_c2_l1": "Landsat 7 ETM+ C2 L1",
        "landsat_etm_c2_l2": "Landsat 7 ETM+ C2 L2",
        "landsat_tm_c2_l1": "Landsat 4-5 TM C2 L1",
        "landsat_tm_c2_l2": "Landsat 4-5 TM C2 L2",
        "landsat_mss_c2_l1": "Landsat 1-5 MSS C2 L1"
    }

    def __init__(
            self,
            username=m2m_credentials.m2m_username,
            password=m2m_credentials.m2m_password,
            logger=logging.getLogger('logger_apiConnector')
    ):
        self.username = username
        self.password = password
        self.logger = logger

    def prepare_download(self):
        os.makedirs('workdir', exist_ok=True)


