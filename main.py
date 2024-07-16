import os
import json
import time
import sqlite3
import logging
import uvicorn
import requests
import threading

from fastapi import FastAPI


con = sqlite3.connect("meteo.db")
cur = con.cursor()
app = FastAPI()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"
data_f = "data/"


def refresh_file(url, fpath, reload, verify_download):
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
            cities.append((
                parts[0].strip(), # id
                parts[1].strip(), # name
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
    forecast_cities = []
    with open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv", "r") as f:
        for l in f.readlines()[1:]:
            parts = l.split(",")
            forecast_cities.append((
                parts[0].strip(), # city_id
                int(parts[1].strip()), # param_id
                parts[2].strip(), # date 
                float(parts[3].strip()) # value
            ))
    # TODO: add last updated (?) add it from the file (?)
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
    if True:
    #try:
        logging.info(f"Triggering refresh")
        valid_new = False
        for ds, reload in datasets.items():
            valid_new = download_resources(ds, reload) or valid_new
        if True or valid_new:
            update_db()
    #except:
    #    logging.info("Refresh failed")
    #finally:
        # TODO: this is kind-of dumb - do I actually want to trigger multiple 
        # timers and keep re-checking files? it does mean that I'll download
        # stuff reasonably quickly if stuff fails for some reason
        timer = threading.Timer(30.0, run_downloads, [datasets])
        timer.start()

run_downloads(target_ds)


# TODO cities.csv actually has coords in it - don't have to look for this in another source
@app.get("/api/v1/forecast/cities") # TODO: take city center coords, and get data within n km (?)
async def download_dataset():
    # TODO: get stuff that's close enough via SQL (?) https://stackoverflow.com/a/67161833
    res = cur.execute("""
        SELECT 
            * 
        FROM 
            forecast_cities
        WHERE
            city_id='P28'
    """)
    return res.fetchall()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
