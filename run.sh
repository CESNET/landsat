#!/bin/bash

docker compose -f ./downloader/docker-compose.yml up -d &
docker compose -f ./http_server/docker-compose.yml up -d &
