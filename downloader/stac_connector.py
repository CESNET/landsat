import datetime
import json
import logging
import os
import random
import time
import requests

import config.stac_config as stac_config

from exceptions.stac_connector import *

"""
Create file ./config/stac_config.py with following content:

base_url = 'https://stac.cesnet.cz'
username = 'your_username_should_go_here'
password = 'your_password_should_go_here'
"""


class STACConnector:
    def __init__(
            self,
            logger=logging.getLogger("STACConnector"),
            username=stac_config.username,
            password=stac_config.password,
            stac_base_url=stac_config.stac_base_url
    ):
        self._logger = logger
        self._stac_base_url = stac_base_url
        self._login(username=username, password=password)

    def _send_request(self, endpoint, payload_dict=None, max_retries=5):
        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = os.path.join(self._stac_base_url, endpoint)

        headers = {}

        if (endpoint != 'auth'):
            if self._api_token_valid_until < datetime.datetime.utcnow():
                self._login(username=self._username, password=self._password)

            headers['Authorization'] = 'Bearer ' + self._stac_token

        data = self._retry_request(
            endpoint=endpoint_full_url, payload_dict=payload_dict,
            max_retries=max_retries, headers=headers
        )

        if data.status_code != 200:
            raise STACRequestNotOK(status_code=data.status_code)

        return data.content

    def _retry_request(self, endpoint, payload_dict, max_retries, headers=None, timeout=10, sleep=5):
        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            self._logger.info('Sending request to URL {}. Retry: {}.'.format(endpoint, retry))
            try:
                if 'auth' in endpoint:
                    response = requests.get(endpoint, auth=(payload_dict['username'], payload_dict['password']))
                else:
                    payload_json = json.dumps(payload_dict)
                    response = requests.post(endpoint, payload_json, headers=headers, timeout=timeout)

                return response

            except requests.exceptions.Timeout:
                self._logger.warning('Connection timeout. Retry number {} of {}.'.format(retry, max_retries))

                retry += 1
                sleep = (1 + random.random()) * sleep
                time.sleep(sleep)

        raise STACRequestTimeout(retry=retry, max_retries=max_retries)

    def _login(self, username=None, password=None):
        if (username is None) or (password is None):
            raise STACCredentialsNotProvided()

        self._username = username
        self._password = password

        self._stac_token = None
        self._api_token_valid_until = datetime.datetime.utcnow() + datetime.timedelta(days=1)

        stac_payload = {
            "username": self._username,
            "password": self._password
        }

        response = self._send_request('auth', stac_payload)
        response_content = json.loads(response)

        self._stac_token = response_content['token']

        if self._stac_token is None:
            raise STACTokenNotObtainedError()

    def register_stac_item(self, json_dict, collection):
        """
        Method invokes POST request on a RESTO server specified in stac_config.base_url and sends there a .json file
        which represents a STAC item.

        Like curl command:
        curl -X POST "https://stac.cesnet.cz"/collections/landsat_ot_c2_l1 \
            -H 'Content-Type: application/json' \
            -H 'Accept: application/json' \
            -H 'Authorization: Bearer stac_auth_token' \
            -d @/path/to/stac_item.json

        :param json_dict: json
        :param collection: collection is currently uploaded Landsat dataset
        :return: nothing
        """

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self._stac_token
        }

        stac_collections_items_url = self._stac_base_url + '/collections' + '/' + collection + '/items'
        response = requests.post(
            url=stac_collections_items_url,
            headers=headers,
            data=json.dumps(json_dict)
        )

        feature_id = json.loads(response.content)

        if 'ErrorCode' in feature_id.keys():
            if feature_id['ErrorCode'] == 409:
                feature_id = self.update_stac_item(json_dict, collection, feature_id['ErrorMessage'].split(' ')[1])
            else:
                raise Exception(f"Error {feature_id['ErrorCode']} for featureId {feature_id}.")

        elif 'errors' in feature_id.keys():
            if len(feature_id['errors']) != 0:
                if feature_id['errors'][0]['code'] == 409:
                    feature_id = self.update_stac_item(
                        json_dict,
                        collection,
                        feature_id['errors'][0]['error'].split(' ')[1]
                    )
                else:
                    raise Exception(f"Error {feature_id['ErrorCode']} for featureId {feature_id}.")

            else:
                feature_id = feature_id['features'][0]['featureId']

        else:
            feature_id = feature_id['features'][0]['featureId']

        self._logger.info(f"Data registered to STAC, url: {stac_collections_items_url}")

        return feature_id

    def update_stac_item(self, json_dict, dataset, feature_id):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self._stac_token
        }

        feature = json_dict['features'][0]

        # TODO check jestli vrátilo HTTP/200-OK
        response = requests.put(
            url=self._stac_base_url + '/collections' + '/' + dataset + '/items' + '/' + feature_id,
            headers=headers,
            data=json.dumps(feature)
        )

        response = json.loads(response.content)

        return feature_id
