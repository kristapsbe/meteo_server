# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

It's been made for use with [this android app](https://github.com/kristapsbe/meteo_android).

## Overview

The server consists of two [docker](https://www.docker.com/) containers. One for the webserver itself, and one for [HAProxy](https://www.haproxy.org/) (this is mostly here for dealing with ssl certificates).

![image](https://github.com/user-attachments/assets/fce12ba9-a93e-4d9e-84d5-7d0d99e8ef13)

[Status page](https://stats.uptimerobot.com/EAWZfpoMkw).

## Start-up

```bash
docker compose down && docker compose build && docker compose up -d
```

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

### DB

The server uses [SQLite](https://www.sqlite.org/) to cache forecast information, and I like using [DBeaver](https://dbeaver.io/download/) if/when I need to poke around the tables.
