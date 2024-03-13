class DownloadedFileError(Exception):
    def __init__(self, message="Downloaded File General Error!", downloaded_file_display_id=None):
        if downloaded_file_display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        self.message = message + f" displayId:{downloaded_file_display_id}"
        super().__init__(self.message)


class DownloadedFileWrongConstructorArgumentsPassed(DownloadedFileError):
    def __init__(self, message="Wrong arguments passed to DownloadedFile constructor!",
                 downloaded_file_display_id=None):
        if downloaded_file_display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        super().__init__(message=message, downloaded_file_display_id=downloaded_file_display_id)


class DownloadedFileWorkdirNotSpecified(DownloadedFileError):
    def __init__(self, message="Workdir not specified!", downloaded_file_display_id=None):
        if downloaded_file_display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        super().__init__(message=message, downloaded_file_display_id=downloaded_file_display_id)


class DownloadedFileS3ConnectorNotSpecified(DownloadedFileError):
    def __init__(self, message="S3Connector not specified!", downloaded_file_display_id=None):
        if downloaded_file_display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        super().__init__(message=message, downloaded_file_display_id=downloaded_file_display_id)


class DownloadedFileSTACConnectorNotSpecified(DownloadedFileError):
    def __init__(self, message="STACConnector not specified!", downloaded_file_display_id=None):
        if downloaded_file_display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        super().__init__(message=message, downloaded_file_display_id=downloaded_file_display_id)
