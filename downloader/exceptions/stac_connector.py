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


class STACRequestMethodNotProvided(STACConnectorError):
    def __init__(self, message="Method not provided for STAC Request!"):
        self.message = message
        super().__init__(self.message)
