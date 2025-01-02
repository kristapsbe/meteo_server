import os
import json
import pytz
import pandas as pd
import sqlite3
import logging
import datetime
import requests

from utils import simlpify_string
from settings import db_file, data_folder, last_updated, run_emergency


skip_download = False

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
    "cols": [
        [{"name": "warning_id", "type": "INTEGER", "pk": True}],
        [{"name": "polygon_id", "type": "INTEGER", "pk": True}],
        [{"name": "lat", "type": "REAL"}],
        [{"name": "lon", "type": "REAL"}],
        [{"name": "order_id", "type": "INTEGER", "pk": True}],
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
    r = requests.get(url, timeout=10)
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


def clean_and_part_line(l):
    return [e[1:-1] if "\"" == e[0] and "\"" == e[-1] else e for e in l.split(",")]


def update_table(t_conf, update_time, db_con):
    logging.info(f"UPDATING '{t_conf["table_name"]}'")
    db_cur = db_con.cursor()
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
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["name"], c["type"])}" for cols in t_conf["cols"] for c in cols])},
            update_time DATEH
            {primary_key_q}
        )
    """)
    upsert_q = "" if len(pks) == 0 else f"""
        ON CONFLICT({", ".join(pks)}) DO UPDATE SET
            {",".join([f"{c["name"]}=excluded.{c["name"]}" for cols in t_conf["cols"] for c in cols if not c.get("pk", False)])},
            update_time={update_time}
    """
    full_q = f"""
        INSERT INTO {t_conf["table_name"]} ({", ".join([c["name"] for cols in t_conf["cols"] for c in cols])}, update_time)
        VALUES ({", ".join(["?"]*len([0 for cols in t_conf["cols"] for _ in cols]))}, {update_time})
        {upsert_q}
    """
    total = len(df.index)
    batch_size = 10000
    batch_count = total//batch_size
    for i in range(batch_count+1):
        db_cur.executemany(full_q, df.iloc[i*batch_size:(i+1)*batch_size].values.tolist())
        logging.info(f"TABLE '{t_conf["table_name"]}' - {db_cur.rowcount} rows upserted (batch {i}/{batch_count}, total {total})")
        db_con.commit()
    db_cur.execute(f"DELETE FROM {t_conf["table_name"]} WHERE update_time < {update_time}")
    logging.info(f"TABLE '{t_conf["table_name"]}' - {db_cur.rowcount} old rows deleted")
    db_con.commit()
    logging.info(f"TABLE '{t_conf["table_name"]}' updated")


def update_warning_bounds_table(update_time, db_con):
    logging.info("UPDATING 'warning_bounds'")
    db_cur = db_con.cursor()
    db_cur.execute("""
        CREATE TABLE IF NOT EXISTS warning_bounds (
            warning_id INTEGER,
            polygon_id INTEGER,
            min_lat REAL,
            max_lat REAL,
            min_lon REAL,
            max_lon REAL,
            update_time DATEH,
            PRIMARY KEY (warning_id, polygon_id)
        )
    """)
    db_cur.execute(f"""
        INSERT INTO warning_bounds (warning_id, polygon_id, min_lat, max_lat, min_lon, max_lon, update_time)
        SELECT
            warning_id,
            polygon_id,
            MIN(lat) as min_lat,
            MAX(lat) as max_lat,
            MIN(lon) as min_lon,
            MAX(lon) as max_lon,
            {update_time} as update_time
        FROM
            warnings_polygons
        GROUP BY
            warning_id, polygon_id
        ON CONFLICT(warning_id, polygon_id) DO UPDATE SET
            min_lat=excluded.min_lat,
            max_lat=excluded.max_lat,
            min_lon=excluded.min_lon,
            max_lon=excluded.max_lon,
            update_time=excluded.update_time
    """)
    logging.info(f"TABLE 'warning_bounds' - {db_cur.rowcount} rows upserted")
    db_con.commit()
    db_cur.execute(f"DELETE FROM warning_bounds WHERE update_time < {update_time}")
    logging.info(f"TABLE 'warning_bounds' - {db_cur.rowcount} old rows deleted")
    db_con.commit()
    logging.info("TABLE 'warning_bounds' updated")


def update_db(update_time):
    upd_con = sqlite3.connect(db_file, timeout=5)
    try:
        for t_conf in table_conf:
            update_table(t_conf, update_time, upd_con)
        update_warning_bounds_table(update_time, upd_con)
        logging.info("DB update finished")
    except BaseException as e:
        logging.info(f"DB update FAILED - {e}")
    finally:
        upd_con.close()


def update_aurora_forecast(update_time): # TODO: cleanup
    url = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
    fpath = f"{data_folder}ovation_aurora_latest.json"
    times_fpath = f"{data_folder}ovation_aurora_times.json"

    r = requests.get(url, timeout=10)
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
            upd_cur.execute("""
                CREATE TABLE IF NOT EXISTS aurora_prob (
                    lon INTEGER,
                    lat INTEGER,
                    aurora INTEGER,
                    update_time DATEH,
                    PRIMARY KEY (lon, lat)
                )
            """)
            upd_cur.executemany(f"""
                INSERT INTO aurora_prob (lon, lat, aurora, update_time)
                VALUES (?, ?, ?, {update_time})
                ON CONFLICT(lon, lat) DO UPDATE SET
                    aurora=excluded.aurora,
                    update_time={update_time}
            """, aurora_data["coordinates"])
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} rows upserted")
            upd_con.commit()
            upd_cur.execute(f"DELETE FROM warning_bounds WHERE update_time < {update_time}")
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} old rows deleted")
            upd_con.commit()
            logging.info("DB update finished")
        except BaseException as e:
            logging.info(f"DB update FAILED - {e}")
        finally:
            upd_con.close()


def run_downloads(datasets):
    logging.info("Triggering refresh")
    skipped_empty = False
    for ds in datasets:
        skipped_empty = download_resources(ds) or skipped_empty

    if skipped_empty and not os.path.isfile(run_emergency):
        open(run_emergency, 'w').write("")

    update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    update_db(update_time)
    update_aurora_forecast(update_time)

    if not skipped_empty:
        open(last_updated, 'w').write(
            datetime.datetime.fromtimestamp(os.path.getmtime(f"{data_folder}{target_ds[0]}.json")).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
        )
        if os.path.isfile(run_emergency):
            os.remove(run_emergency)


if __name__ == "__main__":
    if skip_download:
        update_db()
        update_aurora_forecast()
    else:
        run_downloads(target_ds)
