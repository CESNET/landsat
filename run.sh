#!/bin/bash

bash docker compose up -f ./downloader/docker-compose.yml -d &
bash docker compose up -f ./http-server/docker-compose.yml -d &
