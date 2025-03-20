# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

It's been made for use with [this android app](https://github.com/kristapsbe/meteo_android).

[<img src="https://play.google.com/intl/en_us/badges/images/generic/en-play-badge.png" height="80">](https://play.google.com/store/apps/details?id=lv.kristapsbe.meteo_android)

## Overview

The server consists of two [docker](https://www.docker.com/) containers. One for the webserver itself, and one for [HAProxy](https://www.haproxy.org/).

![image](https://github.com/user-attachments/assets/b074b78b-c43f-4177-8ce7-634a8e302c89)

Status page is available [here](https://stats.uptimerobot.com/EAWZfpoMkw), and aggregated availability metrics are available [here](https://meteo.kristapsbe.lv/api/v1/metrics).

## Setup

### Docker

#### MacOS

https://formulae.brew.sh/cask/rancher

```bash
brew install rancher
```

#### Fedora

https://docs.docker.com/engine/install/fedora/

```bash
dnf remove docker \
    docker-client \
    docker-client-latest \
    docker-common \
    docker-latest \
    docker-latest-logrotate \
    docker-logrotate \
    docker-selinux \
    docker-engine-selinux \
    docker-engine
```

```bash
dnf install dnf-plugins-core
dnf-3 config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

```bash
systemctl enable --now docker
```

### Certs

The default setup expects [ssl certificates](https://letsencrypt.org/) to already be present in the `certs` folder, and it expects `/etc/letsencrypt` to exist on the host

```bash
uv run certbot certonly --standalone
```

```bash
cat /etc/letsencrypt/live/meteo.kristapsbe.lv/fullchain.pem /etc/letsencrypt/live/meteo.kristapsbe.lv/privkey.pem > ~/meteo_server/certs/haproxy.pem
```

### Setting up auto-redeployment

Run `utils/install.sh` to set up a cronjob that checks for new code that has been added to the repos main branch.

When set up I end up in a situation where:
* a download is triggered once every 20 minutes (green),
* an emergency download is potentially triggered every 4 hours, 5 minutes past the hour (yellow),
* code updates are pulled 30 and 50 minutes past the hour (red),
* an OS update is triggered once a day at 2:10 in the morning (blue),
* docker containers are started 15, 25, 35, 45, and 55 minutes past the hour (sand).

<img width="1025" alt="image" src="https://github.com/user-attachments/assets/5b28517b-b023-4a2f-a7f5-db641829bef5" />

### DB

The server uses [SQLite](https://www.sqlite.org/) to cache forecast information, and I like using [DBeaver](https://dbeaver.io/download/) if/when I need to poke around the tables.

## Start-up

```bash
docker compose build --no-cache --build-arg HOST_ARCHITECTURE="$(uname -p)" && docker compose up -d
```

### Notes on running locally

When running the containers locally I find it most convenient to get rid of the certificate volume by removing

```
      - letsencrypt:/etc/letsencrypt
```

from the [docker compose](https://github.com/kristapsbe/meteo_server/blob/main/docker-compose.yml) file.

And to comment out the ssl part in the [haproxy config](https://github.com/kristapsbe/meteo_server/blob/main/haproxy/haproxy.cfg) like so

```
    bind :443 # ssl crt /certs/haproxy.pem
```

Remember that you need to rebuild images after doing this

Example links that can be used to call endpoints can be found in comments at the end of [main.py](https://github.com/kristapsbe/meteo_server/blob/main/app/main.py) (search for `http://localhost:443/`).

## Notes on data selection

### Weather forecasts

Weather forecasts are location specific, and locations are selected either by matching their names, or by finding the closest city to the provided coordintes.

When searching for cities the full set of cities is at first filtered down by selecting
* Republic cities within 10km (red),
* Other cities within 5 km (orange),
* Regional centers within 3.3km (green),
* Parish centers within 2.5km (blue),
* villages within 2km (purple).

![image](https://github.com/user-attachments/assets/cf39f9cf-fb2b-4aa7-b837-fe95644a0ae8)
(defined [here](https://github.com/kristapsbe/meteo_server/blob/06a7f55b07744fa07ea14209aea1d5f6552116e8/app/main.py#L115))

If the filtered set yields no results the closest location by absolute distance is selected.

### Weather warnings

![image](https://github.com/user-attachments/assets/d4199001-3c5e-4c97-af32-5ef505400b78)

Weather warnings are considered relevant if coordinates are within a given warning polygons boundings box (base image screencaptured from [bridinajumi.meteo.lv](https://bridinajumi.meteo.lv/)).

### Assorted notes

```bash
docker exec -it meteo_server-app-1 sh
```

get rid of merged branches

```bash
git checkout main && git fetch -p && git branch -vv | awk '/: gone]/{print $1}' | xargs git branch -d
```

check cert expiry

```bash
openssl x509 -enddate -noout -in file.pem
```
