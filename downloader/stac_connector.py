import datetime
import json
import logging
import random
import time
import requests

from enum import Enum
from urllib.parse import urljoin

import config.stac_config as stac_config

from exceptions.stac_connector import *

"""
Create file ./config/stac_config.py with following content:

base_url = 'https://stac.cesnet.cz'
username = 'your_username_should_go_here'
password = 'your_password_should_go_here'
"""


class Method(Enum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4


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

    def _send_request(self, endpoint, headers=None, payload_dict=None, max_retries=5, method=Method.GET):
        if headers is None:
            headers = {}

        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = urljoin(self._stac_base_url, endpoint)

        if endpoint != 'auth':
            if self._api_token_valid_until < datetime.datetime.utcnow():
                self._login(username=self._username, password=self._password)

            headers['Authorization'] = 'Bearer ' + self._stac_token

        data = self._retry_request(
            endpoint=endpoint_full_url, payload_dict=payload_dict,
            max_retries=max_retries, headers=headers, method=method
        )

        if data.status_code != 200:
            raise STACRequestNotOK(status_code=data.status_code)

        return data.content

    def _retry_request(self, endpoint, payload_dict, max_retries, headers=None, timeout=10, sleep=5, method=Method.GET):
        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            self._logger.info(f'Sending request to URL {endpoint}. Retry: {retry}.')
            try:
                if 'auth' in endpoint:
                    response = requests.get(endpoint, auth=(payload_dict['username'], payload_dict['password']))
                else:
                    payload_json = json.dumps(payload_dict)
                    match method:
                        case Method.GET:
                            response = requests.get(endpoint, payload_json, headers=headers, timeout=timeout)
                        case Method.POST:
                            response = requests.post(endpoint, payload_json, headers=headers, timeout=timeout)
                        case Method.PUT:
                            response = requests.put(endpoint, payload_json, headers=headers, timeout=timeout)
                        case Method.DELETE:
                            response = requests.delete(endpoint, headers=headers, timeout=timeout)
                        case _:
                            raise STACRequestMethodNotProvided()

                return response

            except requests.exceptions.Timeout:
                retry += 1
                self._logger.warning(f"Connection timeout. Retry number {retry} of {max_retries}.")

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

        auth_payload = {
            "username": self._username,
            "password": self._password
        }

        response = self._send_request(endpoint='auth', payload_dict=auth_payload)
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
                feature_id = self.update_stac_item(
                    json_dict,
                    collection,
                    feature_id['ErrorMessage'].split(' ')[1]
                )
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

        self._logger.info(f"Data registered to STAC, url: {stac_collections_items_url}/{feature_id}")

        return feature_id

    def get_stac_item(self, dataset, feature_id):
        headers = {
            'Accept': 'application/json',
        }

        response = self._send_request(
            endpoint=f"/collections/{dataset}/items/{feature_id}",
            headers=headers, method=Method.GET
        )

        feature_dict = json.loads(response)

        return feature_dict

    def update_stac_item(self, json_dict, dataset, feature_id):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self._stac_token
        }

        response = self._send_request(
            endpoint=f"/collections/{dataset}/items/{feature_id}",
            payload_dict=json_dict, headers=headers, method=Method.PUT,
        )

        response = json.loads(response)
        if response['status'] != "success":
            raise STACRequestNotOK(f"STAC Update Request not OK for feature {feature_id}!")

        feature_id = response['message'].split(' ')[-1]
        return feature_id
