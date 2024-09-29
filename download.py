import os
import re
import json
import time
import pandas as pd
import sqlite3
import logging
import requests


# TODO: when set to True this depends on responses containing weather warnings being present on the computer
warning_mode = False
db_f = "meteo.db"
if warning_mode:
    db_f = "meteo_warning_test.db"

con = sqlite3.connect(db_f)

# the cursor doesn't actually do anything in sqlite3, just reusing it
# https://stackoverflow.com/questions/54395773/what-are-the-side-effects-of-reusing-a-sqlite3-cursor
cur = con.cursor()

base_url = "https://data.gov.lv/dati/api/3/"
data_f = "data/"
if warning_mode:
    data_f = "data_warnings/"


def refresh_file(url, fpath, verify_download):
    r = requests.get(url)
    # TODO: there's a damaged .csv - may want to deal with this in a more generic fashion (?)
    r_text = r.content.replace(b'Pressure, (hPa)', b'Pressure (hPa)') if fpath == f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv" else r.content
    if r.status_code == 200 and verify_download(r_text):
        with open(fpath, "wb") as f: # this can be eiher a json or csv
            f.write(r_text)


def verif_json(s):
    try:
        return json.loads(s)['success'] == True
    except:
        return False


verif_funcs = {
    "json": verif_json,
    "csv": lambda s: True
}


def download_resources(ds_name):
    ds_path = f"{data_f}{ds_name}.json"
    refresh_file(f"{base_url}action/package_show?id={ds_name}", ds_path, verif_funcs['json'])
    ds_data = json.loads(open(ds_path, "r").read())
    for r in ds_data['result']['resources']:
        refresh_file(r['url'], f"{data_f}{ds_name}/{r['url'].split('/')[-1]}", verif_funcs['csv'])


target_ds = [
    "hidrometeorologiskie-bridinajumi",
    "meteorologiskas-prognozes-apdzivotam-vietam"
]

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
    

def update_aurora_forecast(reload=900): # TODO: cleanup
    upd_con = sqlite3.connect(db_f)
    try:
        upd_cur = upd_con.cursor()

        url = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
        fpath = "data/ovation_aurora_latest.json"
        times_fpath = "data/ovation_aurora_times.json"

        if not os.path.exists(fpath) or time.time()-os.path.getmtime(fpath) > reload:
            r = requests.get(url)
            if r.status_code == 200:
                with open(fpath, "wb") as f:
                    f.write(r.content)       
                aurora_data = json.loads(r.content)     
                with open(times_fpath, "w") as f:
                    f.write(json.dumps({
                        "Observation Time": aurora_data["Observation Time"], 
                        "Forecast Time": aurora_data["Forecast Time"],
                    }))
                upd_cur.execute(f"DROP TABLE IF EXISTS aurora_prob") # no point in storing old data
                upd_cur.execute(f"""
                    CREATE TABLE aurora_prob (
                        lat INTEGER, 
                        lon INTEGER, 
                        aurora INTEGER
                    )        
                """)
                upd_cur.executemany(f"""
                INSERT INTO aurora_prob (lat, lon, aurora) 
                VALUES (?, ?, ?)
                """, aurora_data["coordinates"])
        else:
            logging.info(f"A recent version of {fpath} exists - not downloading ({int(time.time()-os.path.getmtime(fpath))})")
        upd_con.commit() # TODO: last updared should come from here
        logging.info("aurora table update finished")
    except BaseException as e:
        logging.info(f"aurora table update FAILED - {e}")
    finally:
        upd_con.close()


def run_downloads(datasets):
    logging.info(f"Triggering refresh")
    for ds in datasets:
        download_resources(ds)
    update_db()
    update_aurora_forecast()


if warning_mode:
    update_db()
else:
    run_downloads(target_ds)
