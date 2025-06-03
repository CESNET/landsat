class S3ConnectorError(Exception):
    def __init__(self, message="S3 Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class S3KeyNotSpecified(S3ConnectorError):
    def __init__(self, message="S3 key not provided!"):
        self.message = message
        super().__init__(self.message)

class S3KeyDoesNotExist(S3ConnectorError):
    def __init__(self, message="S3 key does not exist!", key=None):
        if key is not None:
            self.message = f"S3 key {key} does not exist!"
        else:
            self.message = message

        super().__init__(self.message)