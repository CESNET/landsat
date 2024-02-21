import datetime
import json
import logging
import os
import random
import time
import requests

import config.m2m_config as m2m_config


class M2MAPIConnectorError(Exception):
    def __init__(self, message="M2M API Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class M2MAPITokenNotObtainedError(M2MAPIConnectorError):
    def __init__(self, message="M2M API Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class M2MAPICredentialsNotProvided(M2MAPIConnectorError):
    def __init__(self, message="M2M API Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class M2MAPIRequestTimeout(M2MAPIConnectorError):
    def __init__(self, message="M2M API Request Timeouted.", retry=None, max_retries=None):
        if retry is not None:
            self.message = "M2M API Request Timeouted after {} retries.".format(retry)

            if max_retries is not None:
                self.message = self.message + " Max retries: {}.".format(max_retries)
        else:
            self.message = message

        super().__init__(self.message)


class M2MAPIRequestNotOK(M2MAPIConnectorError):
    def __init__(self, message="M2M API Request status code not 200/OK!", status_code=None):
        if status_code is not None:
            self.message = "M2M API Request status code is {}!".format(status_code)
        else:
            self.message = message

        super().__init__(self.message)


class M2MAPIDownloadRequestReturnedFewerURLs(M2MAPIConnectorError):
    def __init__(
            self,
            message="M2M API download-request endpoint returned fewer URLs! entityIds count: {}, URLs count: {}.",
            entity_ids_count=None, urls_count=None
    ):
        if entity_ids_count and urls_count:
            self.message = message.format(entity_ids_count, urls_count)
        else:
            self.message = message


class M2MAPIDownloadableUrlsNotObtained(M2MAPIConnectorError):
    def __init__(self, message="Downloadable URLs not obtained!", downloadable_urls=None):
        self.message = message
        for url in downloadable_urls:
            self.message = self.message + '\n' + str(url)

        super().__init__(self.message)


class M2MAPIConnector:
    def __init__(
            self,
            logger=logging.getLogger("M2MAPIConnector"),
            username=m2m_config.username,
            token=m2m_config.token,
            api_url=m2m_config.api_url
    ):
        self.api_url = api_url
        self.logger = logger
        self.__login_token(username, token)

    def __login_token(self, username=None, token=None):
        if (username is None) or (token is None):
            raise M2MAPICredentialsNotProvided()

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
            raise M2MAPITokenNotObtainedError()

    def scene_search(self, dataset, geojson, day_start, day_end):
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

    def __scene_list_add(self, label, datasetName, entity_ids):
        api_payload = {
            "listId": label,
            "datasetName": datasetName,
            "idField": "entityId",
            "entityIds": entity_ids
        }

        self.__send_request('scene-list-add', api_payload)

    def scene_list_remove(self, label):
        api_payload = {
            "listId": label
        }

        self.__send_request('scene-list-remove', api_payload)

    def __download_options(self, label, dataset):
        api_payload = {
            "listId": label,
            "datasetName": dataset,
            "includeSecondaryFileGroups": "true"
        }

        response = self.__send_request('download-options', api_payload)
        download_options = json.loads(response)

        filtered_download_options = [do for do in download_options['data'] if do['downloadSystem'] == 'dds']

        return filtered_download_options

    def __unique_urls(self, available_urls):
        unique_urls = list({url_dict['url']: url_dict for url_dict in available_urls}.values())
        return unique_urls

    def __download_request(self, download_options):
        available_urls = []

        while True:
            preparing_urls = []

            for download_option in download_options:
                api_payload = {
                    "downloads": [
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id']
                        }
                    ]
                }

                response = self.__send_request('download-request', api_payload)
                download_request = json.loads(response)

                for available_download in download_request['data']['availableDownloads']:
                    available_urls.append(
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id'],
                            "url": available_download['url']
                        }
                    )

                for preparing_download in download_request['data']['preparingDownloads']:
                    preparing_urls.append(
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id'],
                            "url": preparing_download['url']
                        }
                    )

            if not preparing_urls:
                break

            time.sleep(5)

        available_urls = self.__unique_urls(available_urls)

        if len(available_urls) < len(download_options):
            raise M2MAPIDownloadRequestReturnedFewerURLs(
                entity_ids_count=len(download_options), urls_count=len(available_urls)
            )

        return available_urls

    def __get_list_of_urls(self, download_options, entity_display_ids, time_start, time_end, dataset):
        downloadable_urls = self.__download_request(download_options)

        for downloadable_url in downloadable_urls:
            downloadable_url.update(
                {
                    "displayId": entity_display_ids[downloadable_url['entityId']],
                    "dataset": dataset,
                    "start": time_start,
                    "end": time_end
                }
            )

        return downloadable_urls

    def get_downloadable_urls(self, dataset, geojson, time_start, time_end, label="landsat_downloader"):
        self.scene_list_remove(label)

        scenes = self.scene_search(dataset, geojson, time_start, time_end)

        entity_display_ids = {result['entityId']: result['displayId'] for result in scenes['results']}

        self.logger.info(
            "Total hits: {}, records returned: {}, returned IDs: {}".format(
                scenes['totalHits'], scenes['recordsReturned'], entity_display_ids
            )
        )

        if not entity_display_ids:
            return 0

        self.__scene_list_add(label, dataset, list(entity_display_ids.keys()))

        download_options = self.__download_options(label, dataset)

        downloadable_urls = self.__get_list_of_urls(download_options, entity_display_ids, time_start, time_end, dataset)

        return downloadable_urls

    def __send_request(self, endpoint, payload_dict=None, max_retries=5):
        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = str(os.path.join(self.api_url, endpoint))
        payload_json = json.dumps(payload_dict)

        headers = {}

        if (endpoint != 'login') and (endpoint != 'login-token'):
            if self.api_token_valid_until < datetime.datetime.utcnow():
                self.__login_token(self.username, self.token)

            headers['X-Auth-Token'] = self.api_token

        data = self.__retry_request(endpoint_full_url, payload_json, max_retries, headers)

        if data.status_code != 200:
            raise M2MAPIRequestNotOK(status_code=data.status_code)

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
                self.logger.warning('Connection timeout. Retry number {} of {}.'.format(retry, max_retries))

                retry += 1
                sleep = (1 + random.random()) * sleep
                time.sleep(sleep)

        raise M2MAPIRequestTimeout(retry=retry, max_retries=max_retries)
