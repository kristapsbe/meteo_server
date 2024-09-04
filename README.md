# Meteo

A small webserver for caching, filtering, transforming, and serving weather forecast data published by the [Latvian meteorological institute](meteo.lv).

## Installation

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

or uvicorn if multiple workers are desired (suggested - 2*cores)
```
uvicorn main:app --workers 2
```

## Endpoints

Fastapi makes api docs available via `http://localhost:8000/redoc`, the list of available endpoints can be found there.

At the moment I prefer to use this for testing
```
http://localhost:8000/api/v1/forecast/cities?lat=56.8750&lon=23.8658&radius=10
```

## Load testing

```
locust --host http://localhost:8000
```

##

digital ocean setup
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