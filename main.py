import os
import json
import time
import sqlite3
import logging
import uvicorn
import datetime
import requests
import threading

from typing import Annotated
from fastapi import FastAPI, Query


con = sqlite3.connect("meteo.db")
cur = con.cursor()
app = FastAPI(
    title="Meteo",
    version="0.0.1",
)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"
data_f = "data/"


def refresh_file(url, fpath, reload, verify_download): # TODO: I should check the last updated date in the metadata file instead of just pulling files every N minutes
    valid_new = False
    if not os.path.exists(fpath) or time.time()-os.path.getmtime(fpath) > reload:
        logging.info(f"Downloading {fpath}")
        r = requests.get(url)
        if r.status_code == 200 and verify_download(r.content):
            with open(fpath, "wb") as f: # this can be eiher a json or csv
                f.write(r.content)
            valid_new = True
    else:
        logging.info(f"A recent version of {fpath} exists - not downloading ({int(time.time()-os.path.getmtime(fpath))})")
    return valid_new


def verif_json(s):
    try:
        return json.loads(s)['success'] == True
    except:
        return False


verif_funcs = {
    "json": verif_json,
    "csv": lambda s: True
}


def download_resources(ds_name, reload):
    ds_path = f"{data_f}{ds_name}.json"
    valid_new = refresh_file(f"{base_url}action/package_show?id={ds_name}", ds_path, reload, verif_funcs['json'])
    ds_data = {}
    if valid_new: # don't want to download new data csv's unless I get a new datasource json first
        ds_data = json.loads(open(ds_path, "r").read())
        for r in ds_data['result']['resources']:
            refresh_file(r['url'], f"{data_f}{ds_name}/{r['url'].split('/')[-1]}", reload, verif_funcs['csv'])
    return valid_new


target_ds = {
    # TODO: using warnings would be cool - it has both the warning texts and geo polygon that it applies to
    "hidrometeorologiskie-bridinajumi": 900,
    # really nice hourly forecast data
    "meteorologiskas-prognozes-apdzivotam-vietam": 900
}

for ds in target_ds:
    os.makedirs(f"{data_f}{ds}/", exist_ok=True)


def update_db():
    upd_con = sqlite3.connect("meteo.db")
    upd_cur = upd_con.cursor()

    cities = []
    with open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/cities.csv", "r") as f:
        for l in f.readlines()[1:]:
            parts = l.split(",")
            n_part = parts[1].strip()
            cities.append((
                parts[0].strip(), # id
                f"{n_part[0].upper()}{n_part[1:].lower()}", # name
                float(parts[2].strip()), # lat 
                float(parts[3].strip()), # lon
                parts[4].strip() # type
            ))
    upd_cur.execute("""
        CREATE TABLE IF NOT EXISTS cities(
            id TEXT PRIMARY KEY,
            name TEXT,
            lat REAL,
            lon REAL,
            type TEXT
        )        
    """)
    upd_cur.executemany("""
        INSERT OR REPLACE INTO cities (id, name, lat, lon, type) 
        VALUES (?, ?, ?, ?, ?)
    """, cities)
    forecast_cities_params = []
    with open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv", "r") as f:
        for l in f.readlines()[1:]:
            parts = l.split(",")
            forecast_cities_params.append((
                int(parts[0].strip()), # id
                parts[1].strip(), # title_lv
                ",".join(parts[2:]).strip(), # title_en 
            ))
    upd_cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_cities_params(
            id INTEGER PRIMARY KEY,
            title_lv TEXT,
            title_en TEXT
        )        
    """)
    upd_cur.executemany("""
        INSERT OR REPLACE INTO forecast_cities_params (id, title_lv, title_en) 
        VALUES (?, ?, ?)
    """, forecast_cities_params)
    forecast_cities = []
    with open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv", "r") as f:
        for l in f.readlines()[1:]:
            parts = l.split(",")
            forecast_cities.append((
                parts[0].strip(), # city_id
                int(parts[1].strip()), # param_id
                parts[2].strip().replace("-", "").replace(" ", "").replace(":", "")[:10], # date YYYYMMDDHH
                float(parts[3].strip()) # value
            ))
    upd_cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_cities(
            city_id TEXT,
            param_id INTEGER,
            date TEXT,
            value INTEGER,
            PRIMARY KEY (city_id, param_id, date)
        )        
    """)
    upd_cur.executemany("""
        INSERT OR REPLACE INTO forecast_cities (city_id, param_id, date, value) 
        VALUES (?, ?, ?, ?)
    """, forecast_cities)
    upd_con.commit()
    logging.info("DB updated")
    

def run_downloads(datasets):
    try:
        logging.info(f"Triggering refresh")
        valid_new = False
        for ds, reload in datasets.items():
            valid_new = download_resources(ds, reload) or valid_new
        if valid_new:
            update_db()
    except:
        logging.info("Refresh failed")
    finally:
        timer = threading.Timer(30.0, run_downloads, [datasets])
        timer.start()

run_downloads(target_ds)

param_whitelist = [
    'Laika apstākļu piktogramma',
    'Temperatūra (°C)',
    'Sajūtu temperatūra (°C)',
    'Vēja ātrums (m/s)',
    'Vēja virziens (°)',
    'Brāzmas (m/s)',
    'Nokrišņi (mm)',
    'UV indekss (0-10)',
    'Pērkona negaisa varbūtība (%)'
]
param_whitelist_q = "','".join(param_whitelist)


@app.get("/api/v1/forecast/cities")
async def get_city_forecasts(
    lat: Annotated[float, Query(title="Current location (Latitude)")], 
    lon: Annotated[float, Query(title="Current locatino (Longitude)")], 
    radius: Annotated[int, Query(gt=0, lt=50, title="Radius for which to fetch data (km)")] = 25
):
    ''' 
    # City forecast data
    This is a doc string... and automatically will be a published documentation 
    '''
    params = cur.execute(f"""
        SELECT 
            id, title_lv, title_en
        FROM 
            forecast_cities_params
        WHERE
            title_lv in ('{param_whitelist_q}')
    """).fetchall()
    # TODO: should I let a get param set the minimum category of city to return?
    cities = cur.execute(f"""
        SELECT
            id, name, lat, lon, type
        FROM
            cities
        WHERE
            {radius} > ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 
            AND type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs')
    """).fetchall()
    valid_cities_q = "','".join([c[0] for c in cities])
    param_queries = ",".join([f"(SELECT value FROM forecast_cities AS fci WHERE fc.city_id=fci.city_id AND fc.date=fci.date AND param_id={p[0]})" for p in params])
    # TODO: NB hourly data's only provided for the next 24h
    # and the other table contains daily data for the next 7 days https://data.gov.lv/dati/dataset/meteorologiskas-prognozes-apdzivotam-vietam
    # I could fill out the forecasts after the first 24h with data from https://developer.yr.no/
    c_date = datetime.datetime.now().strftime("%Y%m%d%H%M")[:10]
    forecast = cur.execute(f"""
        SELECT 
            city_id, date,
            {param_queries}
        FROM 
            forecast_cities AS fc
        WHERE
            city_id in ('{valid_cities_q}') AND date >= '{c_date}'
        GROUP BY
            city_id, date
    """).fetchall()
    metadata = json.loads(open("data/meteorologiskas-prognozes-apdzivotam-vietam.json", "r").read())
    return {
        "params": [p[1:] for p in params],
        "cities": cities,
        "forecast": forecast,
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
