# landsat

This script is used to obtain, download and register into STAC catalogue Landsat
Imagery data mainly for uses in CESNET, z.s.p.o.

App is divided into two cooperative parts **downloader** and **http-server**.

## downloader

As the name suggest, this component is responsible for downloading data using USGS M2M API.

The data are downloaded one month (30 days) into the past with one day resolution.
The script runs in `while True:` loop, and after downloading all data available at the moment,
it sleeps until the closest 9:00 AM UTC. At this time downloading is re-executed.

### Prerequisites

1) An account and user login token for USGS M2M API must be created.
   Please follow the instructions at [https://m2m.cr.usgs.gov/](https://m2m.cr.usgs.gov/).

2) File **downloader/config/m2m_config.py** must be filled with following information:

```python
api_url = 'https://m2m.cr.usgs.gov/api/api/json/stable/'
username = 'username_used_for_login'
token = 'user_login_token'
```

3) File **downloader/config/s3_config.py** must be filled with following information:

```python
host_base = "https://s3.cl4.du.cesnet.cz"
access_key = "s3_access_key"
secret_key = "s3_secret_key"
host_bucket = "landsat"
```

4) File **downloader/config/stac_config.py** must be filled with following information:

```python
stac_base_url = 'https://stac.cesnet.cz/'
username = 'stac.cesnet.cz username'
password = 'stac.cesnet.cz password'
```

5) You may also want to change contents of **downloader/config/landsat_config.py**, especially
   the `s3_download_host` variable:

```python
s3_download_host = "http://chronos.dhr.cesnet.cz:8081/"
```

The `s3_download_host` must correspond to the computer on which the **http-server** component
is running.

### Logging

Logging can be altered using **downloader/config/landsat_config.py**:

```python
log_directory = "log"
log_name = "landsat.log"
log_level = 20
log_logger = "LandsatLogger"
```

`log_directory` can be either relative to **downloader/** or absolute.

Log is rotated every day at 12:00 AM UTC.

## http-server

Powered by [Sanic](https://sanic.dev/en/).

HTTP server acts as a relay between an asset link published in STAC catalog and S3 storage.

### Prerequisites

The **http-server/.env** file must be filled as follows:

```bash
SANIC__APP_NAME="landsat_http_server"
SANIC__SERVER_HOST="0.0.0.0"
SANIC__SERVER_PORT="8080"

S3_CONNECTOR__HOST_BASE="https://s3.example.com"
S3_CONNECTOR__HOST_BUCKET="landsat"
S3_CONNECTOR__ACCESS_KEY="1234567890ABCDEFGHIJ"
S3_CONNECTOR__SECRET_KEY="123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcde"
```

### Settings

There is not much what can be changed here. The main changes can be done by altering **.env** file.

### Logging

Logging can be altered using **.env** file as well. For example:

```bash
LOGGER__NAME="LandsatHttpServerLogger"
LOGGER__LOG_DIRECTORY="./log"
LOGGER__LOG_FILENAME="landsat_http_server.log"
LOGGER__LOG_LEVEL=20
```

`LOGGER__LOG_DIRECTORY` can be either relative to **http-server/** or absolute.

Log is rotated every day at 12:00 AM UTC.

Log levels are as follows:

| READABLE | INTEGER  |
|----------|----------|
| CRITICAL | 50       |
| FATAL    | CRITICAL |
| ERROR    | 40       |
| WARNING  | 30       |
| WARN     | WARNING  |
| INFO     | 20       |
| DEBUG    | 10       |
| NOTSET   | 0        |

## Running

Package is using Docker. Please see the corresponding **docker-compose.yml** files for [downloader](#downloader)
and [http-server](#http-server).

There is not much to change. In fact just the port of **http-server** in :

```docker
http-server:
    ports:
      - "8080:8080"
```

To run the package just install `docker` and run `docker compose up -d` command in both directories.

So to run the **downloader** in folder `landsat/downloader` execute:

```bash
docker compose up -d
```

and do the same in folder `landsat/http-server` to execute **http-server**.

There is also prepared a little script to run both of these docker containers.

Also in both **docker-compose.yml** files there are flags `restart: unless-stopped`, and thus after rebooting the
machine, scripts will restart automatically.

## Thanks

Using [m2m-api](https://github.com/Fergui/m2m-api) sources by Angel Farguell licensed under MIT License. Many thanks!
