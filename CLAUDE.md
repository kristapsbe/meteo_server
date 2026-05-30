# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A FastAPI weather server that fetches, caches, and serves Latvian (and Lithuanian) meteorological data from public open-data APIs to a companion Android app. Two Docker containers: `app` (Python/FastAPI) and `haproxy` (reverse proxy + TLS termination).

## Commands

**Build and start (production):**
```bash
docker compose build --no-cache --build-arg HOST_ARCHITECTURE="$(uname -p)" && docker compose up -d
```

**Build and run attached (with aurora enabled):**
```bash
make run_attached
```

**Upgrade dependencies in both containers (requires containers to be running):**
```bash
make upgrade_deps
```

**Open a shell in a running container:**
```bash
make terminal_app   # app container
make terminal_hap   # haproxy container
```

**Run a download job manually (inside the app container):**
```bash
sh run_job.sh download_small    # LV forecasts + warnings
sh run_job.sh download_large    # LT forecasts (also runs download_small)
sh run_job.sh download_aurora   # NOAA aurora probability
sh run_job.sh crawl_site        # site crawler
```

**Load testing:**
```bash
cd utils && locust -f locustfile.py
```

**Running locally without TLS:** Remove the `letsencrypt` volume from `docker-compose.yml` and comment out the SSL line in `haproxy/haproxy.cfg` (`bind :443 # ssl crt /certs/haproxy.pem`). Rebuild after any config change.

Example API calls are listed as comments at the bottom of `app/main.py` (search for `http://localhost:443/`).

## Architecture

### Request flow
`client → HAProxy (:443) → app:8000 (FastAPI)`

HAProxy routes `/api/v1/*`, `/privacy-policy`, and `/attribution` to the app. Everything else is not forwarded.

### App container (`app/`)

- **`main.py`** — FastAPI server. Reads from SQLite at startup and serves all endpoints. Endpoints: `GET /api/v1/forecast/cities` (by lat/lon), `GET /api/v1/forecast/cities/name` (fuzzy name search), `GET /api/v1/meta`, `GET /api/v1/version`, `GET /api/v1/metrics`, `/privacy-policy`, `/attribution`.

- **`utils/settings.py`** — Path constants (`data_folder`, `db_file`, etc.). **Not committed.** Generated from `settings.example.py` during Docker build (`COPY utils/settings.example.py utils/settings.py`). The actual `app.env` is also not committed; `app.example.env` is the template.

- **`utils/utils.py`** — `hourly_params` and `daily_params` lists (param IDs used in DB queries) and `simlpify_string()` (multi-script character normalization for fuzzy city name matching).

- **`utils/download_utils.py`** — Shared config: `table_conf` (SQLite schema + CSV source mapping), data source URLs, LT icon code mappings, `col_parsers`/`col_types`.

- **`utils/download_small.py`** — Downloads LV forecast + warning CSVs from `data.gov.lv`, upserts into SQLite, fetches UptimeRobot downtime data. Sets/clears the `run_emergency` sentinel file on failure/success.

- **`utils/download_large.py`** — Downloads LT forecast data from `api.meteo.lt`, then calls `do_20_m_download`. Runs every 4 hours via cron.

- **`utils/download_aurora.py`** — Fetches NOAA aurora probability JSON and populates `aurora_prob` table.

### Cron schedule (inside app container)
- Every 4 h: `download_large` (LT data + LV data)
- Every 20 min (off-peak hours): `download_small` (LV data only)
- Every 4 h + 30 min offset: `download_aurora`
- Every 4 h + 30 min: `crawl_site`

### SQLite database (`/data/meteo.db`)

All datetimes stored as `YYYYMMDDHHMM` integers. Tables:

| Table | Purpose |
|---|---|
| `cities` | Locations (LV + LT). PK: `(id, source)`. |
| `forecast_cities` | Pivoted forecast values. PK: `(city_id, param_id, date)`. |
| `forecast_cities_params` | Param ID → label mapping. |
| `warnings` | Active meteorological warnings. |
| `warnings_polygons` | Warning area polygons (lat/lon points). |
| `warning_bounds` | Denormalized bounding boxes derived from polygons (for fast spatial filtering). |
| `aurora_prob` | NOAA aurora probability by rounded lat/lon. |
| `missing_params` | Cities with incomplete forecast data (currently disabled). |
| `downtimes` | Merged downtime intervals from UptimeRobot logs. |

The app loads a custom SQLite extension (`fuzzy.so` from [sqlean](https://github.com/nalgeon/sqlean)) for `fuzzy_editdist()` used in city name search. Architecture (`arm` vs `x86`) is selected at build time via `HOST_ARCHITECTURE` build arg.

### Emergency mode

When a download fails, `download_small.py` creates `/data/run_emergency`. While this file exists, `main.py` falls back to serving hourly forecasts from the nearest large city (`pilsēta` type) rather than the nearest location. Successful download removes the sentinel file.

### HAProxy container (`haproxy/`)

`haproxy.cfg` is minimal — one frontend, one backend. TLS cert is at `/certs/haproxy.pem` (combined fullchain + privkey). `renew_certs.sh` regenerates it using certbot and is run automatically every two months via cron inside the container.

### Auto-redeployment

`utils/install.sh` installs a host-level cron that polls for main-branch updates and calls `utils/redeploy.sh` every 20 minutes. `utils/update.sh` handles OS updates (runs nightly).
