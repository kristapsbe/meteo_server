# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](https://videscentrs.lvgmc.lv/) to the [Latvian open data portal](https://data.gov.lv/lv).

## Installation


```bash
brew install haproxy
```

```bash
sudo mkdir /etc/haproxy
sudo cp haproxy.cfg /etc/haproxy/haproxy.cfg
haproxy -f /etc/haproxy/haproxy.cfg
```

https://pipx.pypa.io/stable/installation/

set up virtualenv
```bash
pipx install virtualenv
```

create virtual env
```bash
python3 -m venv .venv
```

activate virtual env
```bash
source .venv/bin/activate
```

(optional) check that you're using the right env
```bash
which python
```

install deps
```bash
python -m pip install -r requirements.txt
```

(optional) when done
```bash
deactivate
```

## Startup

either via python for single threaded
```bash
python main.py
```

when running locally you should run the download script in order to get some data to work with
```bash
python download.py
```

or uvicorn if multiple workers are desired (suggested - 2*cores)
```
uvicorn main:app --workers 2
```

## Load testing

```
locust --host http://localhost:8000
```

## digital ocean setup
```bash
dnf install git
git clone https://github.com/kristapsbe/meteo_server.git
#doesnt have make either
git rev-parse HEAD > git.version
dnf install pipx
pipx ensurepath
pipx install virtualenv
python3 -m venv .venv
dnf install screen
screen -d -m python main.py
```


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
dnf install haproxy
```

```bash
mkdir /etc/haproxy/ssl/
cp haproxy.cfg /etc/haproxy/haproxy.cfg
sudo cat /etc/letsencrypt/live/meteo.kristapsbe.lv/fullchain.pem /etc/letsencrypt/live/meteo.kristapsbe.lv/privkey.pem > /etc/haproxy/ssl/haproxy.pem
systemctl stop haproxy
haproxy -f /etc/haproxy/haproxy.cfg
```

```bash
screen -d -m python main.py
screen -d -m haproxy -f /etc/haproxy/haproxy.cfg
```

https://github.com/nalgeon/sqlpkg-cli
```bash
curl -sS https://webi.sh/sqlpkg | sh
sqlpkg install sqlite/spellfix
sqlpkg which sqlite/spellfix

sqlpkg install nalgeon/math
sqlpkg which nalgeon/math
```

crontab https://crontab.guru/
```
crontab -e
```

```
chmod +x python_run.sh
```

```
*/20 * * * * /root/meteo_server/python_run.sh download
10 */4 * * * /root/meteo_server/python_run.sh crawl_site
```
