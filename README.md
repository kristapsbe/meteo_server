# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

## Start-up

```bash
docker compose down && docker compose build && docker compose up -d
```

```bash
docker exec -it meteo_server-app-1 sh
```

## Digital ocean setup

### Docker

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

**TODO: figure out if the update is going to actually work - pretty sure I need to cat the cert files, should make a script for this and use that instead of hte example cron https://certbot.org/renewal-setup**

cert setup
```bash
sudo dnf install python3 augeas-libs
sudo dnf remove certbot
sudo dnf install python3-pip
pip install certbot
ln -s /opt/certbot/bin/certbot /usr/bin/certbot
certbot certonly --standalone
echo "0 0,12 * * * root /opt/certbot/bin/python -c 'import random; import time; time.sleep(random.random() * 3600)' && sudo certbot renew -q" | sudo tee -a /etc/crontab > /dev/null
```

cert upgrade
```bash
pip install --upgrade certbot
```

```bash
mkdir /etc/haproxy/ssl/
cp haproxy.cfg /etc/haproxy/haproxy.cfg
sudo cat /etc/letsencrypt/live/meteo.kristapsbe.lv/fullchain.pem /etc/letsencrypt/live/meteo.kristapsbe.lv/privkey.pem > /etc/haproxy/ssl/haproxy.pem
systemctl stop haproxy
haproxy -f /etc/haproxy/haproxy.cfg
```
