import datetime
import json
import logging
import os
import random
import time

import requests


class APIConnectorError(Exception):
    def __init__(self, message="API Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class APITokenNotObtainedError(APIConnectorError):
    def __init__(self, message="API Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class APICredentialsNotProvided(APIConnectorError):
    def __init__(self, message="API Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class APIRequestTimeout(APIConnectorError):
    def __init__(self, message="API Request Timeouted", retry=None):
        if retry is not None:
            self.message = "API Request Timeouted after {} retries.".format(retry)
        else:
            self.message = message

        super().__init__(self.message)


class APIRequestNotOK(APIConnectorError):
    def __init__(self, message="API Request status code not 200/OK!", status_code=None):
        if status_code is not None:
            self.message = "API Request status code is {}!".format(status_code)
        else:
            self.message = message


class APIConnector:
    api_url = "https://m2m.cr.usgs.gov/api/api/json/stable/"

    def __init__(self, logger=logging.getLogger("APIConnector"), username=None, token=None):
        self.logger = logger
        self.__login_token(username, token)

    def __login_token(self, username=None, token=None):
        if (username is None) or (token is None):
            raise APICredentialsNotProvided()

        self.username = username
        self.token = token

        self.api_token = None
        self.api_token_valid_until = datetime.datetime.utcnow() + datetime.timedelta(hours=2)

        api_payload = {
            "username": self.username,
            "token": self.token
        }

        response = self.__send_request('login-token', api_payload)
        response_content = json.loads(response)

        self.api_token = response_content['data']

        if self.api_token is None:
            raise APITokenNotObtainedError()

    def fetch_scenes(self, dataset, geojson, day_start, day_end):
            api_payload = {
                "maxResults": 10000,
                "datasetName": dataset,
                "sceneFilter": {
                    "spatialFilter": {
                        "filterType": "geojson",
                        "geoJson": geojson
                    },
                    "acquisitionFilter": {
                        "start": str(day_start),
                        "end": str(day_end)
                    }
                }
            }

            response = self.__send_request('scene-search', api_payload)

            scenes = json.loads(response)

            return scenes['data']

    def download_dataset(self, dataset, time_start, time_end):
        geojson_files_paths = [os.path.join('geojson', geojson_file) for geojson_file in os.listdir('geojson')]

        entity_ids = None

        for geojson_file_path in geojson_files_paths:
            with open(geojson_file_path, 'r') as geojson_file:
                geojson = json.loads(geojson_file.read())

            scenes = self.fetch_scenes(dataset, geojson, time_start, time_end)
            entity_ids = [result['entityId'] for result in scenes['results']]

            self.logger.info(
                (
                        "Request for dataset: {},  location: {}, start date: {}, end date: {}. " +
                        "Total hits: {}, records returned: {}, returned IDs: {}"
                ).format(
                    dataset, geojson_file_path, time_start, time_end,
                    scenes['totalHits'], scenes['recordsReturned'], entity_ids
                )
            )

        if entity_ids is None:
            return 0

        return True



    def __send_request(self, endpoint, payload_dict=None, max_retries=5):
        if payload_dict is None:
            payload_dict = {}

        endpoint = str(os.path.join(self.api_url, endpoint))
        payload_json = json.dumps(payload_dict)

        headers = {}

        if (endpoint != 'login') and (endpoint != 'login-token'):
            if self.api_token_valid_until < datetime.datetime.utcnow():
                self.__login_token(self.username, self.token)

            headers['X-Auth-Token'] = self.api_token

        data = self.__retry_request(endpoint, payload_json, max_retries, headers)

        if data.status_code != 200:
            raise APIRequestNotOK(status_code=data.status_code)

        return data.content

    def __retry_request(self, endpoint, payload, max_retries, headers=None, timeout=10, sleep=5):
        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            self.logger.info('Sending request to URL {}. Retry: {}.'.format(endpoint, retry))
            try:
                response = requests.post(endpoint, payload, headers=headers, timeout=timeout)
                return response

            except requests.exceptions.Timeout:
                retry += 1
                logging.info('Connection timeout. Retry number {} of {}.'.format(retry, max_retries))
                sleep = (1 + random.random()) * sleep * 100
                time.sleep(sleep)

        raise APIRequestTimeout(retry=retry)
