# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

It's been made for use with [this android app](https://github.com/kristapsbe/meteo_android).

[<img src="https://play.google.com/intl/en_us/badges/images/generic/en-play-badge.png"
     height="80">](https://play.google.com/store/apps/details?id=lv.kristapsbe.meteo_android)

## Overview

The server consists of two [docker](https://www.docker.com/) containers. One for the webserver itself, and one for [HAProxy](https://www.haproxy.org/) (this is mostly here for dealing with ssl certificates).

![image](https://github.com/user-attachments/assets/e99d866c-eba8-4a4e-85cf-5a525d551fb7)

[Status page](https://stats.uptimerobot.com/EAWZfpoMkw) - the lowest value of the non-`/api/v1/meta` metrics can be used to gauge availability. 99.9% should be possible with 1-2 deployments a day (assuming all else goes well), since building and starting the image takes around 30 seconds, during which the API is unavailable. This will improve if/when I start using a container registry. The `/api/v1/meta` endpoints report data source health.

I'd expect incidents that are shorter than 20 minutes to be deployment related, and incidents that are longer than 20 minutes to mostly be related to the forecast datasoures experiencing issues (and I'd expect the server to be sturdy enough to survive issues like these).

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
cat /etc/letsencrypt/live/meteo.kristapsbe.lv/fullchain.pem /etc/letsencrypt/live/meteo.kristapsbe.lv/privkey.pem > ~/meteo_server/certs/haproxy.pem
```

### Setting up auto-redeployment

```bash
crontab -e
```

```bash
10	*	*	*	*	~/meteo_server/redeploy.sh
30	*	*	*	*	~/meteo_server/redeploy.sh
50	*	*	*	*	~/meteo_server/redeploy.sh
```

### DB

The server uses [SQLite](https://www.sqlite.org/) to cache forecast information, and I like using [DBeaver](https://dbeaver.io/download/) if/when I need to poke around the tables.

## Start-up

```bash
docker compose down && docker compose build && docker compose up -d
```

useful

```bash
docker exec -it meteo_server-app-1 sh
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

### NOTE

I could re-add a version endpont and make the server redeploy itself once a day if changes are available
