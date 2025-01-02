# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

## Start-up

```bash
docker compose down && docker compose build && docker compose up -d
```

```bash
docker exec -it meteo_server-app-1 sh
```

## Setup

### Docker

#### MacOS

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

https://letsencrypt.org/

The default setup expects ssl certificates to already be present in the `certs` folder, and it expects `/etc/letsencrypt` to exist on the host
