import datetime
import json
import logging
import os
import random
import time
import requests

import config.stac_config as stac_config

# import config.downloader_config as downloader_config
# import config.downloader_variables as downloader_variables

"""
Create file ./downloader/config/stac_config.py with following content:

base_url = 'https://stac.cesnet.cz'
username = 'your_username_should_go_here'
password = 'your_password_should_go_here'
templates = 'stac_templates'
download_host = 'http://147.251.115.146:8080/'
"""


class STACConnectorError(Exception):
    def __init__(self, message="STAC Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class STACCredentialsNotProvided(STACConnectorError):
    def __init__(self, message="STAC Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class STACTokenNotObtainedError(STACConnectorError):
    def __init__(self, message="STAC Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class STACRequestTimeout(STACConnectorError):
    def __init__(self, message="STAC Request Timeouted", retry=None, max_retries=None):
        if retry is not None:
            self.message = "STAC Request Timeouted after {} retries.".format(retry)

            if max_retries is not None:
                self.message = self.message + " Max retries: {}.".format(max_retries)
        else:
            self.message = message

        super().__init__(self.message)


class STACRequestNotOK(STACConnectorError):
    def __init__(self, message="STAC Request status code not 200/OK!", status_code=None):
        if status_code is not None:
            self.message = "STAC Request status code is {}!".format(status_code)
        else:
            self.message = message

        super().__init__(self.message)


class STACConnector:
    def __init__(
            self,
            logger=logging.getLogger("STACConnector"),
            username=stac_config.username,
            password=stac_config.password,
            templates_dir=stac_config.templates_dir,
            stac_base_url=stac_config.stac_base_url
    ):
        self.logger = logger
        self.templates_dir = templates_dir
        self.stac_base_url = stac_base_url
        self.__login(username=username, password=password)

    def __send_request(self, endpoint, payload_dict=None, max_retries=5):
        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = os.path.join(self.stac_base_url, endpoint)

        headers = {}

        if (endpoint != 'auth'):
            if self.api_token_valid_until < datetime.datetime.utcnow():
                self.__login(username=self.username, password=self.password)

            headers['Authorization'] = 'Bearer ' + self.stac_token

        data = self.__retry_request(
            endpoint=endpoint_full_url, payload_dict=payload_dict,
            max_retries=max_retries, headers=headers
        )

        if data.status_code != 200:
            raise STACRequestNotOK(status_code=data.status_code)

        return data.content

    def __retry_request(self, endpoint, payload_dict, max_retries, headers=None, timeout=10, sleep=5):
        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            self.logger.info('Sending request to URL {}. Retry: {}.'.format(endpoint, retry))
            try:
                if 'auth' in endpoint:
                    response = requests.get(endpoint, auth=(payload_dict['username'], payload_dict['password']))
                else:
                    payload_json = json.dumps(payload_dict)
                    response = requests.post(endpoint, payload_json, headers=headers, timeout=timeout)

                return response

            except requests.exceptions.Timeout:
                self.logger.warning('Connection timeout. Retry number {} of {}.'.format(retry, max_retries))

                retry += 1
                sleep = (1 + random.random()) * sleep
                time.sleep(sleep)

        raise STACRequestTimeout(retry=retry, max_retries=max_retries)

    def __login(self, username=None, password=None):
        if (username is None) or (password is None):
            raise STACCredentialsNotProvided()

        self.username = username
        self.password = password

        self.stac_token = None
        self.api_token_valid_until = datetime.datetime.utcnow() + datetime.timedelta(days=1)

        stac_payload = {
            "username": self.username,
            "password": self.password
        }

        response = self.__send_request('auth', stac_payload)
        response_content = json.loads(response)

        self.stac_token = response_content['data']

        if self.stac_token is None:
            raise STACTokenNotObtainedError()

    @staticmethod
    def _is_leap_year(year):
        """
        Method detects if given year is leap year

        :param year: year
        :return: True if year is leap, otherwise False
        """
        if (
                (year % 400 == 0) or
                (year % 100 != 0) and
                (year % 4 == 0)
        ):
            return True
        else:
            return False

    @staticmethod
    def _save_json_to_file(json_dictionary, output_filename):
        """
        Method saves dictionary which represents a JSON into .json file.

        :param json_dictionary: dictionary representing a JSON
        :param output_filename: path to .json filename into which the JSON is being saved
        :return: True
        """

        with open(output_filename, "w") as output_file:
            output_file.write(json.dumps(json_dictionary, indent=4))

        return True

    def generate_feature_json(self, year, month, dataset, working_filename):
        """
        Method generates a dictionary which represents JSON STAC item.
        It uses the template located in stac_templates [feature]<dataset>.json. This template is partially ready
        to be published, but some variables needs to be filled in. That is what this method does.

        :param year: year which is being catalogued
        :param month: month which is being catalogued
        :param dataset: dataset which is being catalogued
        :param working_filename: filename into which the JSON will be saved
        :return: nothing
        """

        self.logger.info("Creating STAC JSON for data; year=" + year + ", month=" + month + ", dataset=" + dataset)

        # Opening JSON template into Python dictionary feature_json
        with open(self.templates_dir + '/' + '[feature]' + dataset + '.json') as json_file:
            json_content = json_file.read()
        feature_json = json.loads(json_content)

        for product_type in downloader_variables.product_types[dataset]:
            dataset_href = (stac_config.download_host + year + '/' + month + '/' + dataset + '/' +
                            product_type + downloader_config.data_format['extension'])
            feature_json['features'][0]['assets'][product_type.replace("_", "-")]['href'] = dataset_href

        feature_id = year + "-" + month + "-" + dataset
        feature_json['features'][0]['id'] = feature_id

        url_to_self = stac_config.base_url + "collections/" + dataset + "/items/" + feature_id
        feature_json['features'][0]['links'][0]['href'] = url_to_self

        start_datetime = year + "-" + month + "-01T00:00:00Z"
        feature_json['features'][0]['properties']['start_datetime'] = start_datetime

        end_datetime = year + "-" + month
        match month:
            case "01" | "03" | "05" | "07" | "08" | "10" | "12":
                end_datetime += "-31T23:00:00Z"
            case "04" | "06" | "09" | "11":
                end_datetime += "-30T23:00:00Z"
            case "02":
                if self._is_leap_year(int(year)):
                    end_datetime += "-29T23:00:00Z"
                else:
                    end_datetime += "-28T23:00:00Z"
            case _:
                raise Exception("Unknown month: " + month)
        feature_json['features'][0]['properties']['end_datetime'] = end_datetime

        feature_json['features'][0]['properties']['datetime'] = start_datetime

        # Saving JSON dictionary feature_json into .json file working_filename
        if not self._save_json_to_file(feature_json, working_filename):
            raise Exception("Error when writing JSON to file: ", working_filename)

    def register_stac_item(self, path_to_json, dataset):
        """
        Method invokes POST request on a RESTO server specified in stac_config.base_url and sends there a .json file
        which represents a STAC item.

        Like curl command:
        curl -X POST "https://stac.cesnet.cz"/collections/reanalysis-era5-single-levels \
            -H 'Content-Type: application/json' \
            -H 'Accept: application/json' \
            -H 'Authorization: Bearer stac_auth_token' \
            -d @/path/to/stac_item.json

        :param path_to_json: Path to POSTed .json file
        :param dataset: ERA5 dataset which is represented by POSTed .json file
        :return: nothing
        """

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self.stac_token
        }

        data = open(path_to_json)
        response = requests.post(self.stac_base_url + '/collections' + '/' + dataset + '/items',
                                 headers=headers, data=data)

        self.logger.info("Data registered to STAC; registered json=" + path_to_json + ", response=" + str(response))
