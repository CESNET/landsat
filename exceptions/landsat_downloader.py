class LandsatDownloaderError(Exception):
    def __init__(self, message="Landsat Downloader General Error!"):
        self.message = message
        super().__init__(self.message)


class LandsatDownloaderUrlDoNotContainsFilename(LandsatDownloaderError):
    def __init__(self, message="URL does not return filename!", url=None):
        if url is not None:
            self.message = message + " " + str(url)
        else:
            self.message = message

        super().__init__(self.message)


class LandsatDownloaderDownloadedFileHasDifferentSize(LandsatDownloaderError):
    def __init__(
            self, message="Downloaded file size not matching expected file size!",
            content_length=None, file_size=None
    ):
        self.message = message + " Content-length: " + str(content_length) + ", file size: " + str(file_size) + "."
        super().__init__(self.message)
