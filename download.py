import os
import json
import pytz
import pandas as pd
import sqlite3
import logging
import datetime
import requests

from utils import simlpify_string
from settings import db_file, data_folder, warning_mode


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"

target_ds = [
    "hidrometeorologiskie-bridinajumi",
    "meteorologiskas-prognozes-apdzivotam-vietam"
]

for ds in target_ds:
    os.makedirs(f"{data_folder}{ds}/", exist_ok=True)

col_parsers = {
    "TEXT": lambda r: str(r).strip(),
    "TITLE_TEXT": lambda r: str(r).strip().title(),
    "CLEANED_TEXT": lambda r: simlpify_string(str(r).strip().lower()),
    "INTEGER": lambda r: int(str(r).strip()),
    "REAL": lambda r: float(str(r).strip()),
    # TODO: do I really need minutes? - would mean that I consistently work with datetime strings that are YYYYMMDDHHMM
    "DATEH": lambda r: str(r).strip().replace(".", "").replace("-", "").replace(" ", "").replace(":", "").ljust(12, "0")[:12] # YYYYMMDDHHMM
}

col_types = {
    "DATEH": "TEXT"
}

table_conf = [{
    "files": [{
        "name": f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/cities.csv",
        "skip_if_empty": True
    }],
    "table_name": "cities",
    "cols": [
        [{"name": "id", "type": "TEXT", "pk": True}],
        [{"name": "name", "type": "TITLE_TEXT"}, {"name": "search_name", "type": "CLEANED_TEXT"}],
        [{"name": "lat", "type": "REAL"}],
        [{"name": "lon", "type": "REAL"}],
        [{"name": "type", "type": "TEXT"}],
    ],
},{
    "files": [{
        "name": f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv",
        "skip_if_empty": True
    }],
    "table_name": "forecast_cities_params",
    "cols": [
        [{"name": "id", "type": "INTEGER", "pk": True}],
        [{"name": "title_lv", "type": "TEXT"}],
        [{"name": "title_en", "type": "TEXT"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities_day.csv",
        "skip_if_empty": True
    }, {
        "name": f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv",
        "skip_if_empty": True,
        "do_emergency_dl": True
    }],
    "table_name": "forecast_cities",
    "cols": [
        [{"name": "city_id", "type": "TEXT", "pk": True}],
        [{"name": "param_id", "type": "INTEGER", "pk": True}],
        [{"name": "date", "type": "DATEH", "pk": True}],
        [{"name": "value", "type": "REAL"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}hidrometeorologiskie-bridinajumi/novadi.csv"
    }],
    "table_name": "municipalities",
    "cols": [
        [{"name": "id", "type": "INTEGER", "pk": True}],
        [{"name": "name_lv", "type": "TEXT"}],
        [{"name": "name_en", "type": "TEXT"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}hidrometeorologiskie-bridinajumi/bridinajumu_novadi.csv"
    }],
    "table_name": "warnings_municipalities",
    "cols": [
        [{"name": "warning_id", "type": "INTEGER"}],
        [{"name": "municipality_id", "type": "INTEGER"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}hidrometeorologiskie-bridinajumi/bridinajumu_poligoni.csv"
    }],
    "table_name": "warnings_polygons",
    "cols": [ # TODO: figure out why the pks failed
        [{"name": "warning_id", "type": "INTEGER"}], # "pk": True}],
        [{"name": "polygon_id", "type": "INTEGER"}], # "pk": True}],
        [{"name": "lat", "type": "REAL"}],
        [{"name": "lon", "type": "REAL"}],
        [{"name": "order_id", "type": "INTEGER"}], # "pk": True}],
    ]
},{ # TODO: partial at the moment - finish this
    "files": [{
        "name": f"{data_folder}hidrometeorologiskie-bridinajumi/bridinajumu_metadata.csv"
    }],
    "table_name": "warnings",
    "cols": [
        [{"name": "number", "type": "TEXT", "pk": True}],
        [{"name": "id", "type": "INTEGER", "pk": True}],
        [{"name": "intensity_lv", "type": "TEXT"}],
        [{"name": "intensity_en", "type": "TEXT"}],
        [{"name": "regions_lv", "type": "TEXT"}],
        [{"name": "regions_en", "type": "TEXT"}],
        [{"name": "type_lv", "type": "TEXT"}],
        [{"name": "type_en", "type": "TEXT"}],
        [{"name": "time_from", "type": "DATEH"}],
        [{"name": "time_to", "type": "DATEH"}],
        [{"name": "description_lv", "type": "TEXT"}],
        [{"name": "description_en", "type": "TEXT"}],
    ]
}]


def refresh_file(url, fpath, verify_download):
    r = requests.get(url)
    # TODO: there's a damaged .csv - may want to deal with this in a more generic fashion (?)
    r_text = r.content.replace(b'Pressure, (hPa)', b'Pressure (hPa)') if fpath == f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv" else r.content

    curr_conf = [f_conf for conf in table_conf for f_conf in conf["files"] if fpath in f_conf["name"]]
    skip_if_empty = False
    do_emergency_dl = False
    if len(curr_conf) > 0:
        skip_if_empty = curr_conf[0].get("skip_if_empty", False)
        do_emergency_dl = curr_conf[0].get("do_emergency_dl", False)

    if r.status_code == 200 and verify_download(r_text, skip_if_empty):
        with open(fpath, "wb") as f: # this can be eiher a json or csv
            f.write(r_text)
        return False
    else:
        return do_emergency_dl


def verif_json(s, _):
    try:
        return json.loads(s)['success'] == True
    except:
        return False


verif_funcs = {
    "json": verif_json,
    "csv": lambda s, skip_if_empty: (len(s.split(b'\n')) > 2 or not skip_if_empty)
}


def download_resources(ds_name):
    ds_path = f"{data_folder}{ds_name}.json"
    refresh_file(f"{base_url}action/package_show?id={ds_name}", ds_path, verif_funcs['json'])
    ds_data = json.loads(open(ds_path, "r").read())

    skipped_empty = False
    for r in ds_data['result']['resources']:
        skipped_empty = refresh_file(r['url'], f"{data_folder}{ds_name}/{r['url'].split('/')[-1]}", verif_funcs['csv']) or skipped_empty
    return skipped_empty


def update_table(t_conf, db_cur):
    logging.info(f"UPDATING '{t_conf["table_name"]}'")
    df = None
    for data_file in t_conf["files"]:
        tmp_df = pd.read_csv(data_file["name"]).dropna()
        for ct in range(len(t_conf["cols"])):
            for col in t_conf["cols"][ct]:
                tmp_df[f"_new_{col["name"]}"] = tmp_df[tmp_df.columns[ct]].apply(col_parsers[col["type"]])
        tmp_df = tmp_df[[f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols]]
        if df is None:
            df = pd.DataFrame(tmp_df)
        else:
            df = pd.concat([df, tmp_df])

    # TODO: FIX
    group_keys = [f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols if c.get("pk", False)]
    if len(group_keys) > 0:
        df = df.groupby(group_keys).max().reset_index() # getting rid of duplicates

    pks = [c["name"] for cols in t_conf["cols"] for c in cols if c.get("pk", False)]
    primary_key_q = "" if len(pks) < 1 else f", PRIMARY KEY ({", ".join(pks)})"
    db_cur.execute(f"DROP TABLE IF EXISTS {t_conf["table_name"]}") # no point in storing old data
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["name"], c["type"])}" for cols in t_conf["cols"] for c in cols])}
            {primary_key_q}
        )
    """)
    db_cur.executemany(f"""
        INSERT INTO {t_conf["table_name"]} ({", ".join([c["name"] for cols in t_conf["cols"] for c in cols])})
        VALUES ({", ".join(["?"]*len([0 for cols in t_conf["cols"] for _ in cols]))})
    """, df.values.tolist())
    logging.info(f"TABLE '{t_conf["table_name"]}' updated")


def update_warning_bounds_table(db_cur):
    logging.info(f"UPDATING 'warning_bounds'")
    db_cur.execute(f"DROP TABLE IF EXISTS warning_bounds") # no point in storing old data
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS warning_bounds (
            warning_id,
            polygon_id,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            PRIMARY KEY (warning_id, polygon_id)
        )
    """)
    db_cur.execute(f"""
        INSERT INTO warning_bounds (warning_id, polygon_id, min_lat, max_lat, min_lon, max_lon)
        SELECT
            warning_id,
            polygon_id,
            MIN(lat) as min_lat,
            MAX(lat) as max_lat,
            MIN(lon) as min_lon,
            MAX(lon) as max_lon
        FROM
            warnings_polygons
        GROUP BY
            warning_id, polygon_id
    """)
    logging.info(f"TABLE 'warning_bounds' updated")


def update_db():
    upd_con = sqlite3.connect(db_file)
    try:
        upd_cur = upd_con.cursor()
        for t_conf in table_conf:
            # TODO: check if I should make a cursor and commit once, or once per function call
            update_table(t_conf, upd_cur)
        update_warning_bounds_table(upd_cur)
        upd_con.commit() # TODO: last updared should come from here
        logging.info("DB update finished")
    except BaseException as e:
        logging.info(f"DB update FAILED - {e}")
    finally:
        upd_con.close()


def update_aurora_forecast(): # TODO: cleanup
    url = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
    fpath = "data/ovation_aurora_latest.json"
    times_fpath = "data/ovation_aurora_times.json"

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

        upd_con = sqlite3.connect(db_file)
        upd_cur = upd_con.cursor()
        try:
            upd_cur.execute(f"DROP TABLE IF EXISTS aurora_prob") # no point in storing old data
            upd_cur.execute(f"""
                CREATE TABLE aurora_prob (
                    lon INTEGER,
                    lat INTEGER,
                    aurora INTEGER
                )
            """)
            upd_cur.executemany(f"""
                INSERT INTO aurora_prob (lon, lat, aurora)
                VALUES (?, ?, ?)
            """, aurora_data["coordinates"])
            upd_con.commit()
            logging.info("DB update finished")
        except BaseException as e:
            logging.info(f"DB update FAILED - {e}")
        finally:
            upd_con.close()


def run_downloads(datasets):
    logging.info(f"Triggering refresh")
    skipped_empty = False
    for ds in datasets:
        skipped_empty = download_resources(ds) or skipped_empty

    if skipped_empty and not os.path.isfile('run_emergency'):
        open('run_emergency', 'w').write("")

    update_db()
    update_aurora_forecast()

    if not skipped_empty: # moving here in case the db updates blow up
        open('last_updated', 'w').write(
            datetime.datetime.fromtimestamp(os.path.getmtime(f"{data_folder}{target_ds[0]}.json")).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
        )
        if os.path.isfile('run_emergency'):
            os.remove('run_emergency')


if __name__ == "__main__":
    if warning_mode:
        update_db()
        update_aurora_forecast()
    else:
        run_downloads(target_ds)
