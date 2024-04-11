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

HTTP server acts as a relay between asset link published in STAC catalog and S3 storage.
Using [awscli](https://du.cesnet.cz/cs/navody/object_storage/cesnet_s3_url_share) the script 
generates a temporary link to download selected asset.

### Prerequisites

The **http-server/.env** file must be filled as follows:

```bash
AWS_CONFIG_FILE=~/.aws/config
AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials
AWS_ACCESS_KEY_ID=ASDFGHJKLQWERTYUIOP1
AWS_SECRET_ACCESS_KEY=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd
AWS_DEFAULT_REGION= 
AWS_DEFAULT_OUTPUT=text
```

#### WARNING

`AWS_DEFAULT_REGION= ` got space after **=**, and it must be there as said [here](https://du.cesnet.cz/cs/navody/object_storage/awscli/start).

### Settings

There is not much what can be changed here. See beginning of **http-server/main.py**.

Lines:
```python
host_name = "0.0.0.0"
server_port = 8080
```
could be altered, but I suggest to change settings of the [Docker](#Running) and firewall.

### Logging

Logging can be altered using **http-server/main.py** lines:

```python
log_logger = "HttpServerLogger"
log_directory = './log'
log_name = 'http-server.log'
log_level = 20
```

`log_directory` can be either relative to **http-server/** or absolute.

Log is rotated every day at 12:00 AM UTC.

## Running

Package is using Docker. Please see the **./docker-compose.yml**. As can be seen there,
two services are executed: **downloader** and **http-server**.

There is not much to change. In fact just the port of **http-server**:
```docker
http-server:
    ports:
      - "8080:8080"
```

## Thanks

Using [m2m-api](https://github.com/Fergui/m2m-api) by Angel Farguell licensed under MIT License. Many thanks!