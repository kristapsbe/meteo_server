import os
import json
import pytz
import time
import pandas as pd
import sqlite3
import logging
import uvicorn
import datetime
import requests
import threading

from typing import Annotated
from fastapi import FastAPI, Query


# TODO: when set to True this depends on responses containing weather warnings being present on the computer
warning_mode = False
db_f = "meteo.db"
if warning_mode:
    db_f = "meteo_warning_test.db"

# TODO: don't keep a global db con open - open it up when necessary
con = sqlite3.connect(db_f)
cur = con.cursor()
# https://semver.org/
# Given a version number MAJOR.MINOR.PATCH, increment the:
#    MAJOR version when you make incompatible API changes
#    MINOR version when you add functionality in a backward compatible manner
#    PATCH version when you make backward compatible bug fixes
app = FastAPI(
    title="Meteo",
    version="0.1.0",
    docs_url=None,
    redoc_url=None
)
git_commit = open("git.version", "r").read().strip()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"
data_f = "data/"
if warning_mode:
    data_f = "data_warnings/"


def refresh_file(url, fpath, reload, verify_download):
    valid_new = False
    if not os.path.exists(fpath) or time.time()-os.path.getmtime(fpath) > reload:
        logging.info(f"Downloading {fpath}")
        r = requests.get(url)
        # TODO: there's a damaged .csv - may want to deal with this in a more generic fashion (?)
        r_text = r.content.replace(b'Pressure, (hPa)', b'Pressure (hPa)') if fpath == f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv" else r.content
        if r.status_code == 200 and verify_download(r_text):
            with open(fpath, "wb") as f: # this can be eiher a json or csv
                f.write(r_text)
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
    "hidrometeorologiskie-bridinajumi": 900,
    "meteorologiskas-prognozes-apdzivotam-vietam": 900
}

for ds in target_ds:
    os.makedirs(f"{data_f}{ds}/", exist_ok=True)

col_parsers = {
    "TEXT": lambda r: str(r).strip(),
    "TITLE_TEXT": lambda r: str(r).strip().title(),
    "INTEGER": lambda r: int(str(r).strip()),
    "REAL": lambda r: float(str(r).strip()),
    # TODO: do I really need minutes? - would mean that I consistently work with datetime strings that are YYYYMMDDHHMM
    "DATEH": lambda r: str(r).strip().replace(".", "").replace("-", "").replace(" ", "").replace(":", "").ljust(12, "0")[:12] # YYYYMMDDHHMM
}

col_types = {
    "DATEH": "TEXT"
}

table_conf = [{
    "files": [f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/cities.csv"],
    "table_name": "cities",
    "cols": [
        {"name": "id", "type": "TEXT", "pk": True},
        {"name": "name", "type": "TITLE_TEXT"},
        {"name": "lat", "type": "REAL"},
        {"name": "lon", "type": "REAL"},
        {"name": "type", "type": "TEXT"},
    ]
},{
    "files": [f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv"],
    "table_name": "forecast_cities_params",
    "cols": [
        {"name": "id", "type": "INTEGER", "pk": True},
        {"name": "title_lv", "type": "TEXT"},
        {"name": "title_en", "type": "TEXT"},
    ]
},{
    "files": [
        f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities_day.csv",
        f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv"
    ],
    "table_name": "forecast_cities",
    "cols": [
        {"name": "city_id", "type": "TEXT", "pk": True},
        {"name": "param_id", "type": "INTEGER", "pk": True},
        {"name": "date", "type": "DATEH", "pk": True},
        {"name": "value", "type": "REAL"},
    ]
},{
    "files": [f"{data_f}hidrometeorologiskie-bridinajumi/novadi.csv"],
    "table_name": "municipalities",
    "cols": [
        {"name": "id", "type": "INTEGER", "pk": True},
        {"name": "name_lv", "type": "TEXT"},
        {"name": "name_en", "type": "TEXT"},
    ]
},{
    "files": [f"{data_f}hidrometeorologiskie-bridinajumi/bridinajumu_novadi.csv"],
    "table_name": "warnings_municipalities",
    "cols": [
        {"name": "warning_id", "type": "INTEGER"},
        {"name": "municipality_id", "type": "INTEGER"},
    ]
},{
    "files": [f"{data_f}hidrometeorologiskie-bridinajumi/bridinajumu_poligoni.csv"],
    "table_name": "warnings_polygons",
    "cols": [
        {"name": "warning_id", "type": "INTEGER", "pk": True},
        {"name": "polygon_id", "type": "INTEGER", "pk": True},
        {"name": "lat", "type": "REAL"},
        {"name": "lon", "type": "REAL"},
        {"name": "order_id", "type": "INTEGER", "pk": True},
    ]
},{ # TODO: partial at the moment - finish this
    "files": [f"{data_f}hidrometeorologiskie-bridinajumi/bridinajumu_metadata.csv"],
    "table_name": "warnings",
    "cols": [
        {"name": "number", "type": "TEXT", "pk": True},
        {"name": "id", "type": "INTEGER", "pk": True},
        {"name": "intensity_lv", "type": "TEXT"},
        {"name": "intensity_en", "type": "TEXT"},
        {"name": "regions_lv", "type": "TEXT"},
        {"name": "regions_en", "type": "TEXT"},
        {"name": "type_lv", "type": "TEXT"},
        {"name": "type_en", "type": "TEXT"},
        {"name": "time_from", "type": "DATEH"},
        {"name": "time_to", "type": "DATEH"},
        {"name": "description_lv", "type": "TEXT"},
        {"name": "description_en", "type": "TEXT"},
    ]
}]


def update_table(t_conf, db_cur):
    logging.info(f"UPDATING '{t_conf["table_name"]}'")
    df = None
    for data_file in t_conf["files"]:
        tmp_df = pd.read_csv(data_file).dropna()
        for ct in range(len(t_conf["cols"])):
            tmp_df[f"_new_{t_conf["cols"][ct]["name"]}"] = tmp_df[tmp_df.columns[ct]].apply(col_parsers[t_conf["cols"][ct]["type"]])
        tmp_df = tmp_df[[f"_new_{c["name"]}" for c in t_conf["cols"]]]
        if df is None:
            df = pd.DataFrame(tmp_df)
        else:
            df = pd.concat([df, tmp_df])
    db_cur.execute(f"DROP TABLE IF EXISTS {t_conf["table_name"]}") # no point in storing old data
    pks = [c["name"] for c in t_conf["cols"] if c.get("pk", False)]
    primary_key_q = "" if len(pks) < 1 else f", PRIMARY KEY ({", ".join(pks)})"
    db_cur.execute(f"""
        CREATE TABLE {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["name"], c["type"])}" for c in t_conf["cols"]])}
            {primary_key_q}
        )        
    """)
    db_cur.executemany(f"""
        INSERT INTO {t_conf["table_name"]} ({", ".join([c["name"] for c in t_conf["cols"]])}) 
        VALUES ({", ".join(["?"]*len(t_conf["cols"]))})
    """, df.values.tolist())
    logging.info(f"TABLE '{t_conf["table_name"]}' updated")


def update_db():
    upd_con = sqlite3.connect(db_f)
    try:
        upd_cur = upd_con.cursor()
        for t_conf in table_conf:
            # TODO: check if I should make a cursor and commit once, or once per function call
            update_table(t_conf, upd_cur)
        upd_con.commit() # TODO: last updared should come from here
        logging.info("DB update finished")
    except BaseException as e:
        logging.info(f"DB update FAILED - {e}")
    finally:
        upd_con.close()
    

def run_downloads(datasets, refresh_timer=30.0):
    try:
        logging.info(f"Triggering refresh")
        valid_new = False
        for ds, reload in datasets.items():
            valid_new = download_resources(ds, reload) or valid_new
        if valid_new:
            update_db()
    except BaseException as e: # https://docs.python.org/3/library/exceptions.html#exception-hierarchy
        logging.info(f"Refresh failed - {e}")
    finally:
        timer = threading.Timer(refresh_timer, run_downloads, [datasets])
        timer.start()


if warning_mode:
    update_db()
else:
    run_downloads(target_ds)

hourly_params = [
    'Laika apstākļu piktogramma',
    'Temperatūra (°C)',
    'Sajūtu temperatūra (°C)',
    'Vēja ātrums (m/s)',
    'Vēja virziens (°)',
    'Brāzmas (m/s)',
    'Nokrišņi (mm)',
    'UV indekss (0-10)',
    'Pērkona negaisa varbūtība (%)',
]
hourly_params_q = "','".join(hourly_params)

daily_params = [
    'Diennakts vidējā vēja vērtība (m/s)',
    'Diennakts maksimālā vēja brāzma (m/s)',
    'Diennakts maksimālā temperatūra (°C)',
    'Diennakts minimālā temperatūra (°C)',
    'Diennakts nokrišņu summa (mm)',
    'Diennakts nokrišņu varbūtība (%)',
    'Laika apstākļu piktogramma nakti',
    'Laika apstākļu piktogramma diena',
]
daily_params_q = "','".join(daily_params)


def get_params(cur, param_q):
    return cur.execute(f"""
        SELECT 
            id, title_lv, title_en
        FROM 
            forecast_cities_params
        WHERE
            title_lv in ('{param_q}')
    """).fetchall()


def get_closest_city(cur, lat, lon, distance=15, max_distance=100):
    cities = cur.execute(f"""
        WITH city_distances AS (
            SELECT
                id,
                name,
                CASE type
                    WHEN 'republikas pilseta' THEN 1
                    WHEN 'citas pilsētas' THEN 2
                    WHEN 'rajona centrs' THEN 3
                    WHEN 'pagasta centrs' THEN 4
                END as ctype,
                ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 as distance
            FROM
                cities
            WHERE
                type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs')
        )
        SELECT
            id, name, ctype, distance
        FROM
            city_distances
        WHERE
            distance <= {distance}
        ORDER BY
            ctype ASC, distance ASC
    """).fetchall()
    if len(cities) == 0:
        if distance < max_distance:
            return get_closest_city(cur, lat, lon, distance+5)
        else:
            return ()
    else:
        return cities[0]
    

def get_forecast(cur, city, c_date, params):
    if len(city) == 0:
        return []
    param_queries = ",".join([f"(SELECT value FROM forecast_cities AS fci WHERE fc.city_id=fci.city_id AND fc.date=fci.date AND param_id={p[0]}) AS val_{p[0]}" for p in params])
    param_where = " OR ".join([f"val_{p[0]} IS NOT NULL" for p in params])
    return cur.execute(f"""
        WITH h_temp AS (
            SELECT 
                city_id, date,
                {param_queries}
            FROM 
                forecast_cities AS fc
            WHERE
                city_id = '{city[0]}' AND date >= '{c_date}'
            GROUP BY
                city_id, date                    
        )
        SELECT * FROM h_temp WHERE {param_where}
    """).fetchall()


def get_warnings(cur, lat, lon):
    # TODO: turning the warning polygons into big squares - this should at least work - should use the actual poly bounds at some point
    relevant_warnings = cur.execute(f"""
        WITH warning_bounds AS (
            SELECT
                warning_id,
                MIN(lat) as min_lat,
                MAX(lat) as max_lat,
                MIN(lon) as min_lon,
                MAX(lon) as max_lon
            FROM
                warnings_polygons
            GROUP BY
                warning_id
        )
        SELECT warning_id FROM warning_bounds WHERE {lat} >= min_lat AND {lat} <= max_lat AND {lon} >= min_lon AND {lon} <= max_lon
    """).fetchall()
    warnings = []
    if len(relevant_warnings) > 0:
        warnings = cur.execute(f"""
            SELECT DISTINCT
                id,
                intensity_lv,
                intensity_en,
                regions_lv,
                regions_en,
                type_lv,
                type_en,
                time_from,
                time_to,
                description_lv,
                description_en
            FROM
                warnings
            WHERE
                id in ({", ".join([str(w[0]) for w in relevant_warnings])})
        """).fetchall()
    return warnings


# http://localhost:8000/api/v1/forecast/cities?lat=56.9730&lon=24.1327
@app.get("/api/v1/forecast/cities")
async def get_city_forecasts(lat: float, lon: float):
    h_params = get_params(cur, hourly_params_q)
    d_params = get_params(cur, daily_params_q)
    city = get_closest_city(cur, lat, lon)
    c_date = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    if warning_mode:
        c_date = "202407270000"
    h_forecast = get_forecast(cur, city, c_date, h_params)
    d_forecast = get_forecast(cur, city, c_date, d_params)
    warnings = get_warnings(cur, lat, lon)
    metadata_f = f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam.json"
    metadata = json.loads(open(metadata_f, "r").read())
    return {
        "hourly_params": [p[1:] for p in h_params],
        "daily_params": [p[1:] for p in d_params],
        "city": str(city[1]) if len(city) > 0 else "",
        "hourly_forecast": [{
            "time": f[1],
            "vals": f[2:]
        } for f in h_forecast],
        "daily_forecast": [{
            "time": f[1],
            "vals": f[2:]
        } for f in d_forecast],
        "warnings": [{
            "id": w[0],
            "intensity": w[1:3],
            "regions": w[3:5],
            "type": w[5:7],
            "time": w[7:9],
            "description": w[9:]
        } for w in warnings],
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
        # TODO: get local timezone instead
        "last_downloaded": datetime.datetime.fromtimestamp(os.path.getmtime(metadata_f)).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"),
    }


# http://localhost:8000/api/v1/version
@app.get("/api/v1/version")
async def get_version():
    return {
        "version": app.version,
        "commit": git_commit
    }


# http://localhost:8000/api/v1/forecast/test_ctemp?temp=13.2
@app.get("/api/v1/forecast/test_ctemp")
async def get_test_ctemp(temp: float):
    metadata_f = f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam.json"
    metadata = json.loads(open(metadata_f, "r").read())
    return {
        "hourly_params": [], 
        "daily_params": [], 
        "city": {
            "name": "Piņķi"
        },
        "hourly_forecast": [{
            "time": 202407292100,
            "vals": [2103.0, temp, 13.7, 9.5, 320.0, 19.2, 0.0, 0.0, 0.0]
        }],
        "daily_forecast": [],
        "warnings": [],
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
        # TODO: get local timezone instead
        "last_downloaded": datetime.datetime.fromtimestamp(os.path.getmtime(metadata_f)).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
