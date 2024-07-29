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


# TODO: delete this once you've figured out what to do with weather warnings
warning_mode = True

con = sqlite3.connect("meteo.db")
cur = con.cursor()
app = FastAPI(
    title="Meteo",
    version="0.0.1",
)
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

col_parsers = {
    "TEXT": lambda s: s.strip(),
    "CAPITTEXT": lambda s: s.strip(),
    "INTEGER": lambda s: int(s.strip()),
    "REAL": lambda s: float(s.strip()),
    "DATEH": lambda s: s.strip().replace("-", "").replace(" ", "").replace(":", "")[:10] # YYYYMMDDHH
}

col_types = {
    "DATEH": "TEXT"
}

table_conf = [{
    "files": [f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/cities.csv"],
    "table_name": "cities",
    "cols": [
        {"name": "id", "type": "TEXT", "pk": True},
        {"name": "name", "type": "TEXT"},
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
    ]
}]


def update_table(t_conf, db_cur):
    vals = []
    for data_file in t_conf["files"]:
        with open(data_file, "r") as f:
            curr_line = ""
            for l in f.readlines()[1:]:
                curr_line = f"{curr_line}{l}"
                # bridinajumu_metadata contains newlines in "" - just merging the lines for now
                if len(curr_line.split('"')) % 2 == 1:
                    parts = curr_line.split(",") # TODO - need to account for entries that are wrapped in ""
                    tmp = []
                    for c in range(len(t_conf["cols"])):
                        # TODO: forcity_param has a broken line (it has an extra comma in it) 
                        # come up with a nice way to deal with it
                        tmp.append(col_parsers[t_conf["cols"][c]["type"]](parts[c]))
                    vals.append(tmp)
                    curr_line = ""
    pks = [c["name"] for c in t_conf["cols"] if c.get("pk", False)]
    primary_key_q = "" if len(pks) < 1 else f", PRIMARY KEY ({", ".join(pks)})"
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["name"], c["type"])}" for c in t_conf["cols"]])}
            {primary_key_q}
        )        
    """)
    db_cur.executemany(f"""
        INSERT OR REPLACE INTO {t_conf["table_name"]} ({", ".join([c["name"] for c in t_conf["cols"]])}) 
        VALUES ({", ".join(["?"]*len(t_conf["cols"]))})
    """, vals)
    print(f"TABLE '{t_conf["table_name"]}' updated")


def update_db():
    upd_con = sqlite3.connect("meteo.db")
    upd_cur = upd_con.cursor()
    for t_conf in table_conf:
        update_table(t_conf, upd_cur)
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


# TODO: get forecasts per region from national weather agencies before trips (?)
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
    h_params = cur.execute(f"""
        SELECT 
            id, title_lv, title_en
        FROM 
            forecast_cities_params
        WHERE
            title_lv in ('{hourly_params_q}')
    """).fetchall()
    d_params = cur.execute(f"""
        SELECT 
            id, title_lv, title_en
        FROM 
            forecast_cities_params
        WHERE
            title_lv in ('{daily_params_q}')
    """).fetchall()
    # TODO: should I let a get param set the minimum category of city to return?
    cities = cur.execute(f"""
        SELECT
            id, name, type, lat, lon
        FROM
            cities
        WHERE
            {radius} > ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 
            AND type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs')
    """).fetchall()
    valid_cities_q = "','".join([c[0] for c in cities])
    c_date = datetime.datetime.now().strftime("%Y%m%d%H%M")[:10] # TODO: I could just ommit the %M, right?
    if warning_mode:
        c_date = "2024072900"
    h_param_queries = ",".join([f"(SELECT value FROM forecast_cities AS fci WHERE fc.city_id=fci.city_id AND fc.date=fci.date AND param_id={p[0]}) AS val_{p[0]}" for p in h_params])
    h_param_where = " OR ".join([f"val_{p[0]} IS NOT NULL" for p in h_params])
    h_forecast = cur.execute(f"""
        WITH h_temp AS (
            SELECT 
                city_id, date,
                {h_param_queries}
            FROM 
                forecast_cities AS fc
            WHERE
                city_id in ('{valid_cities_q}') AND date >= '{c_date}'
            GROUP BY
                city_id, date                    
        )
        SELECT * FROM h_temp WHERE {h_param_where}
    """).fetchall()
    d_param_queries = ",".join([f"(SELECT value FROM forecast_cities AS fci WHERE fc.city_id=fci.city_id AND fc.date=fci.date AND param_id={p[0]}) AS val_{p[0]}" for p in d_params])
    d_param_where = " OR ".join([f"val_{p[0]} IS NOT NULL" for p in d_params])
    d_forecast = cur.execute(f"""     
        WITH d_temp AS (
            SELECT 
                city_id, date,
                {d_param_queries}
            FROM 
                forecast_cities AS fc
            WHERE
                city_id in ('{valid_cities_q}') AND date >= '{c_date}'
            GROUP BY
                city_id, date
        )
        SELECT * FROM d_temp WHERE {d_param_where}
    """).fetchall()
    relevant_warnings = cur.execute(f"""
        SELECT DISTINCT
            warning_id
        FROM
            warnings_polygons
        WHERE
            {radius} > ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 
    """).fetchall()
    warnings = []
    if len(relevant_warnings) > 0:
        warnings = cur.execute(f"""
            SELECT DISTINCT
                intensity_lv,
                intensity_en,
                regions_lv,
                regions_en,
                type_lv,
                type_en
            FROM
                warnings
            WHERE
                id in ({", ".join([str(w[0]) for w in relevant_warnings])})
        """).fetchall()
    metadata = json.loads(open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam.json", "r").read())
    return {
        "hourly_params": [p[1:] for p in h_params], # don't need the id col, getting rid of it
        "daily_params": [p[1:] for p in d_params], # don't need the id col, getting rid of it
        "cities": [{
            "id": str(c[0]),
            "name": str(c[1]),
            "type": str(c[2]),
            "coords": {
                "lat": c[3],
                "lon": c[4]
            }
        } for c in cities],
        "hourly_forecast": [{
            "id": f[0],
            "time": f[1],
            "vals": f[2:]
        } for f in h_forecast],
        "daily_forecast": [{
            "id": f[0],
            "time": f[1],
            "vals": f[2:]
        } for f in d_forecast],
        "warnings": [{
            "intensity": w[:2],
            "regions": w[2:4],
            "type": w[4:]
        } for w in warnings],
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
