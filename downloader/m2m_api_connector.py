import datetime
import json
import logging
import os
import random
import time
import requests

import config.m2m_config as m2m_config

from exceptions.m2m_api_connector import *


class M2MAPIConnector:
    def __init__(
            self,
            logger=logging.getLogger("M2MAPIConnector"),
            username=m2m_config.username,
            token=m2m_config.token,
            api_url=m2m_config.api_url
    ):
        self._api_url = api_url
        self._logger = logger
        self._login_token(username, token)

    def _login_token(self, username=None, token=None):
        if (username is None) or (token is None):
            raise M2MAPICredentialsNotProvided()

        self._username = username
        self._token = token

        self._api_token = None
        self._api_token_valid_until = datetime.datetime.utcnow() + datetime.timedelta(hours=2)

        api_payload = {
            "username": self._username,
            "token": self._token
        }

        response = self._send_request('login-token', api_payload)
        response_content = json.loads(response)

        self._api_token = response_content['data']

        if self._api_token is None:
            raise M2MAPITokenNotObtainedError()

    def _scene_search(self, dataset, geojson, day_start, day_end):
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

        response = self._send_request('scene-search', api_payload)
        scenes = json.loads(response)

        return scenes['data']

    def _scene_list_add(self, label, datasetName, entity_ids):
        api_payload = {
            "listId": label,
            "datasetName": datasetName,
            "idField": "entityId",
            "entityIds": entity_ids
        }

        self._send_request('scene-list-add', api_payload)

    def scene_list_remove(self, label):
        api_payload = {
            "listId": label
        }

        self._send_request('scene-list-remove', api_payload)

    def _download_options(self, label, dataset):
        api_payload = {
            "listId": label,
            "datasetName": dataset,
            "includeSecondaryFileGroups": "true"
        }

        response = self._send_request('download-options', api_payload)
        download_options = json.loads(response)

        """
        # Fixed below
        filtered_download_options = [do for do in download_options['data'] if do['downloadSystem'] == 'dds']
        """

        filtered_download_options = []
        for download_option in download_options['data']:
            if download_option['downloadSystem'] == 'dds' and download_option['available'] == True:
                filtered_download_options.append(download_option)
            elif download_option['downloadSystem'] == 'ls_zip' and download_option['available'] == True:
                filtered_download_options.append(download_option)

        return filtered_download_options

    def _unique_urls(self, available_urls):
        unique_urls = list({url_dict['url']: url_dict for url_dict in available_urls}.values())
        return unique_urls

    def _download_request(self, download_options):
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

                response = self._send_request('download-request', api_payload)
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

        available_urls = self._unique_urls(available_urls)

        if len(available_urls) < len(download_options):
            raise M2MAPIDownloadRequestReturnedFewerURLs(
                entity_ids_count=len(download_options), urls_count=len(available_urls)
            )

        return available_urls

    def _get_list_of_files(self, download_options, entity_display_ids, time_start, time_end, dataset):
        downloadable_urls = self._download_request(download_options)

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

    def get_downloadable_files(self, dataset, geojson, time_start, time_end, label="landsat_downloader"):
        self.scene_list_remove(label)

        scenes = self._scene_search(dataset, geojson, time_start, time_end)

        entity_display_ids = {result['entityId']: result['displayId'] for result in scenes['results']}

        self._logger.info(
            f"Total hits: {scenes['totalHits']}, records returned: {scenes['recordsReturned']}, " +
            f"returned IDs: {entity_display_ids}"
        )

        if not entity_display_ids:
            return []

        self._scene_list_add(label, dataset, list(entity_display_ids.keys()))

        download_options = self._download_options(label, dataset)

        downloadable_files = self._get_list_of_files(download_options, entity_display_ids, time_start, time_end,
                                                     dataset)
        for downloadable_file in downloadable_files:
            downloadable_file.update({'geojson': geojson})

        return downloadable_files

    def _send_request(self, endpoint, payload_dict=None, max_retries=5):
        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = str(os.path.join(self._api_url, endpoint))
        payload_json = json.dumps(payload_dict)

        headers = {}

        if (endpoint != 'login') and (endpoint != 'login-token'):
            if self._api_token_valid_until < datetime.datetime.utcnow():
                self._login_token(self._username, self._token)

            headers['X-Auth-Token'] = self._api_token

        data = self._retry_request(endpoint_full_url, payload_json, max_retries, headers)

        if data.status_code != 200:
            raise M2MAPIRequestNotOK(status_code=data.status_code)

        return data.content

    def _retry_request(self, endpoint, payload, max_retries=5, headers=None, timeout=10, sleep=5):
        """
        Method sends request to specified endpoint until number of max_retries is reached
        For max_retries=5 the request is sent 6 times, since first (or the "zeroth") is understood as proper request.

        :param endpoint: URL of USGS M2M API endpoint
        :param payload: JSON string of a API payload
        :param max_retries: default 5 retries
        :param headers: dict of headers sent to M2M API endpoint
        :param timeout: default 10 seconds
        :param sleep: wait seconds between retries, default 5 seconds
        :return: request response, bytestring of response, can be parsed to JSON
        :raise M2MAPIRequestTimeout: when limit of max_retries is reached
        """

        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            self._logger.info('Sending request to URL {}. Retry: {}.'.format(endpoint, retry))
            try:
                response = requests.post(endpoint, payload, headers=headers, timeout=timeout)
                return response

            except requests.exceptions.Timeout:
                self._logger.warning('Connection timeout. Retry number {} of {}.'.format(retry, max_retries))

                retry += 1
                sleep = (1 + random.random()) * sleep
                time.sleep(sleep)

        raise M2MAPIRequestTimeout(retry=retry, max_retries=max_retries)
