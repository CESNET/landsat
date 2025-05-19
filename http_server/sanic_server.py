import logging
import mimetypes

from sanic import Sanic, response, exceptions

from s3_connector import S3Connector

from config.variables import *


class SanicServer():
    _logger: logging.Logger = None
    _app: Sanic = None

    def __init__(self, logger=logging.Logger(SANIC__APP_NAME)):
        self._logger = logger

        self._s3_connector = S3Connector(logger=self._logger)

        self._app = Sanic(SANIC__APP_NAME)
        self.register_routes()

    def run(self):
        self._app.run(host=SANIC__SERVER_HOST, port=SANIC__SERVER_PORT)

    def get_app(self):
        return self._app

    async def _stream_body(self, response_body):
        chunk_size = 8192
        while True:
            chunk = response_body.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def register_routes(self):
        @self._app.get("/<path:path>")
        async def parser(request, path):
            self._logger.info(
                f"[{str(request.id)}]; "
                f"ClientIP: {request.client_ip}; "
                f"PathArgs: {request.raw_url.decode('utf-8')}"
            )

            if "landsat" not in path:
                self._logger.info(
                    f"[{str(request.id)}]; "
                    f"Landsat not present, returning 404"
                )
                return response.json({"error": "Not found"}, status=404)

            try:
                s3_key = path.replace(f"{S3_CONNECTOR__HOST_BUCKET}/", "").lstrip("/")
                tar_member_file = request.args.get("tarMemberFile")
                offset = int(request.args.get("offset") or 0)
                size = int(request.args.get("size") or 0)

                if (
                        (tar_member_file is not None) and
                        (offset != 0) and
                        (size != 0)
                ):
                    response_body = self._s3_connector.fetch_from_tar_by_range(key=s3_key, offset=offset, size=size)

                    self._logger.info(
                        f"[{str(request.id)}]; "
                        f"Response streaming file: {tar_member_file}; "
                        f"Size: {size} bytes"
                    )
                    return response.raw(
                        response_body, content_type=mimetypes.guess_type(tar_member_file)[0] or "application/octet-stream",
                        headers={
                            "Content-Disposition": f"attachment; filename={tar_member_file}",
                            "Content-Length": str(size)
                        }
                    )

                else:
                    fileshare_url = self._s3_connector.generate_fileshare_url(key=s3_key)

                    self._logger.info(f"[{str(request.id)}] Redirecting to: {fileshare_url}")
                    return response.redirect(fileshare_url)
            except Exception as e:
                self._logger.error(f"[{str(request.id)}] Exception occurred: {str(e)}")
                return response.json({"error": "Bad request"}, status=400)
