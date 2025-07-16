import os
import json
import pytz
import pandas as pd
import sqlite3
import logging
import datetime
import requests

from time import sleep
from utils import simlpify_string, hourly_params, daily_params
from settings import db_file, data_folder, data_uptimerobot_folder, last_updated, run_emergency, run_emergency_failed


skip_download = False

logging.basicConfig(
    filename='/data/download.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

base_url = "https://data.gov.lv/dati/api/3/"

warning_s = "hidrometeorologiskie-bridinajumi"
forecast_s = "meteorologiskas-prognozes-apdzivotam-vietam-jaunaka-datu-kopa"

MIN_PARAM_COUNT = min(len(hourly_params), len(daily_params))
target_ds = [warning_s, forecast_s]

for ds in target_ds:
    os.makedirs(f"{data_folder}{ds}/", exist_ok=True)

col_parsers = {
    "TEXT": lambda r: str(r).strip(),
    "TITLE_TEXT": lambda r: str(r).strip().title(),
    "CLEANED_TEXT": lambda r: simlpify_string(str(r).strip().lower()),
    "INTEGER": lambda r: int(str(r).strip()),
    "REAL": lambda r: float(str(r).strip()),
    # TODO: do I really need minutes? - would mean that I consistently work with datetime strings that are YYYYMMDDHHMM
    # TODO: revert when the source gets fixed
    # "DATEH": lambda r: str(r).strip().replace(".", "").replace("-", "").replace(" ", "").replace(":", "").ljust(12, "0")[:12] # YYYYMMDDHHMM
    "DATEH": lambda r: str(r).strip().replace(".", "").replace("-", "").replace(" ", "").replace(":", "").replace("T", "").ljust(12, "0")[:12], # YYYYMMDDHHMM
    "CONST_LV": lambda _: "LV",
}

col_types = {
    "DATEH": "INTEGER",
    "CONST_LV": "TEXT",
    "TITLE_TEXT": "TEXT",
    "CLEANED_TEXT": "TEXT",
}

table_conf = [{
    "files": [{
        "name": f"{data_folder}{forecast_s}/cities.csv",
        "skip_if_empty": True
    }],
    "table_name": "cities",
    "cols": [
        [{"name": "id", "type": "TEXT", "pk": True}, {"name": "source", "type": "CONST_LV", "pk": True}],
        [{"name": "name", "type": "TITLE_TEXT"}, {"name": "search_name", "type": "CLEANED_TEXT"}],
        [{"name": "lat", "type": "REAL"}],
        [{"name": "lon", "type": "REAL"}],
        [{"name": "type", "type": "TEXT"}],
        [{"name": "county", "type": "TEXT"}, {"name": "country", "type": "CONST_LV"}],
    ],
},{
    "files": [{
        "name": f"{data_folder}{forecast_s}/forcity_param.csv",
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
        "name": f"{data_folder}{forecast_s}/forecast_cities_day.csv",
        "skip_if_empty": True
    }, {
        "name": f"{data_folder}{forecast_s}/forecast_cities.csv",
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
        "name": f"{data_folder}{warning_s}/novadi.csv"
    }],
    "table_name": "municipalities",
    "cols": [
        [{"name": "id", "type": "INTEGER", "pk": True}],
        [{"name": "name_lv", "type": "TEXT"}],
        [{"name": "name_en", "type": "TEXT"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}{warning_s}/bridinajumu_novadi.csv"
    }],
    "table_name": "warnings_municipalities",
    "cols": [
        [{"name": "warning_id", "type": "INTEGER"}],
        [{"name": "municipality_id", "type": "INTEGER"}],
    ]
},{
    "files": [{
        "name": f"{data_folder}{warning_s}/bridinajumu_poligoni.csv"
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
        "name": f"{data_folder}{warning_s}/bridinajumu_metadata.csv"
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
    r_text = r.content.replace(b'""Lidosta', b'"Lidosta').replace(b'ga""""', b'ga"""').replace(b'rupes nov.""', b'rupes nov."') if fpath == f"{data_folder}{forecast_s}/cities.csv" else r.content
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
        logging.error(f"{fpath} failed (status code {r.status_code})")
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
        if ds_name == warning_s:
            # TODO: get rid of this when the source gets fixed
            skipped_empty = refresh_file(f"https://data.gov.lv/dati/lv/datastore/dump/{r['id']}?format=csv", f"{data_folder}{ds_name}/{r['url'].split('/')[-1]}", verif_funcs['csv']) or skipped_empty
        else:
            skipped_empty = refresh_file(r['url'], f"{data_folder}{ds_name}/{r['url'].split('/')[-1]}", verif_funcs['csv']) or skipped_empty

    return skipped_empty


def clean_and_part_line(l):
    return [e[1:-1] if "\"" == e[0] and "\"" == e[-1] else e for e in l.split(",")]


def update_table(t_conf, update_time, db_con):
    logging.info(f"UPDATING '{t_conf["table_name"]}'")
    db_cur = db_con.cursor()
    df = None

    for data_file in t_conf["files"]:
        tmp_df = pd.read_csv(data_file["name"]).dropna(how='all')

        if t_conf["table_name"] in {"warnings_municipalities", "warnings_polygons", "warnings"}:
            tmp_df = tmp_df[tmp_df.columns[1:]] # TODO: delete when the source gets fixed

        for ct in range(len(t_conf["cols"])):
            for col in t_conf["cols"][ct]:
                tmp_df[f"_new_{col["name"]}"] = tmp_df[tmp_df.columns[ct]].apply(col_parsers[col["type"]])
        tmp_df = tmp_df[[f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols]]
        if df is None:
            df = pd.DataFrame(tmp_df)
        else:
            df = pd.concat([df, tmp_df])

    group_keys = [f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols if c.get("pk", False)]
    if len(group_keys) > 0: # getting rid of duplicates
        df = df.groupby(group_keys).max().reset_index()[[f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols]]

    pks = [c["name"] for cols in t_conf["cols"] for c in cols if c.get("pk", False)]
    primary_key_q = "" if len(pks) < 1 else f", PRIMARY KEY ({", ".join(pks)})"
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["type"], c["type"])}" for cols in t_conf["cols"] for c in cols])},
            update_time INTEGER
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
    batch_size = 10000 # value selected arbitrarily - small enough to make batches fast, big enough to fit non-forecast data into a single batch
    batch_count = total//batch_size
    for i in range(batch_count+1):
        db_cur.executemany(full_q, df.iloc[i*batch_size:(i+1)*batch_size].values.tolist())
        logging.info(f"TABLE '{t_conf["table_name"]}' - {db_cur.rowcount} rows upserted (batch {i}/{batch_count}, total {total})")
        db_con.commit()
    if t_conf["table_name"] == "forecast_cities":
        if False:
            # clocks getting turned can mess the data up - leaving this as a contingency in case I just need to clear out all of the old data
            # TODO: it may be worth dealing with this in a more automated fashion
            db_cur.execute(f"DELETE FROM {t_conf["table_name"]} WHERE update_time < {update_time}")
        else:
            # dealing with cases when a single forecast param may have gone missing
            not_params = f"param_id NOT IN ({",".join([str(p) for p in hourly_params+daily_params])})"
            h_where = f"param_id IN ({",".join([str(p) for p in hourly_params])})"
            d_where = f"param_id IN ({",".join([str(p) for p in daily_params])})"
            h_valid_dates = db_cur.execute(f"""
                SELECT MIN(date), MAX(date) FROM {t_conf["table_name"]} WHERE update_time = {update_time} AND {h_where}
            """).fetchall() # better than getting all dates, but still slow
            d_valid_dates = db_cur.execute(f"""
                SELECT MIN(date), MAX(date) FROM {t_conf["table_name"]} WHERE update_time = {update_time} AND {d_where}
            """).fetchall() # better than getting all dates, but still slow
            db_cur.execute(f"""
                DELETE FROM {t_conf["table_name"]}
                WHERE
                    ((date < {h_valid_dates[0][0]} OR date > {h_valid_dates[0][1]}) AND {h_where}) OR
                    ((date < {d_valid_dates[0][0]} OR date > {d_valid_dates[0][1]}) AND {d_where}) OR
                    {not_params} OR param_id IS NULL
            """)
        logging.info(f"TABLE '{t_conf["table_name"]}' - {db_cur.rowcount} old rows deleted")
        db_con.commit()

        logging.info("UPDATING 'missing_params'")
        db_cur.execute("""
            CREATE TABLE IF NOT EXISTS missing_params (
                city_id TEXT,
                name TEXT,
                type TEXT,
                update_time INTEGER
            )
        """)
        missing_params = db_cur.execute(f"""
            WITH filtered_forecasts AS (
               	SELECT
              		COUNT(param_id) AS param_id, date, city_id , MAX(update_time) AS update_time
               	FROM
              		forecast_cities
               	GROUP BY
              		date, city_id
            )
            SELECT
               	city_id, c.name, c."type"
            FROM
                filtered_forecasts f JOIN cities c on f.city_id = c.id
            WHERE
               	param_id < {MIN_PARAM_COUNT}
            GROUP BY
               	city_id
        """).fetchall()
        db_cur.executemany(f"""
            INSERT INTO missing_params (city_id, name, type, update_time)
            VALUES (?, ?, ?, {update_time})
        """, missing_params)
        logging.info(f"TABLE 'missing_params' - {db_cur.rowcount} rows upserted")
        db_con.commit()
        db_cur.execute(f"DELETE FROM missing_params WHERE update_time < {update_time}")
        logging.info(f"TABLE 'missing_params' - {db_cur.rowcount} old rows deleted")
        db_con.commit()
        logging.info("TABLE 'missing_params' updated")
    else:
        db_cur.execute(f"DELETE FROM {t_conf["table_name"]} WHERE update_time < {update_time} LIMIT {batch_size}")
        logging.info(f"TABLE '{t_conf["table_name"]}' - {db_cur.rowcount} old rows deleted")
        while db_cur.rowcount > 0:
            db_cur.execute(f"DELETE FROM {t_conf["table_name"]} WHERE update_time < {update_time} LIMIT {batch_size}")
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
            update_time INTEGER,
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
        logging.error(f"DB update FAILED - {e}")
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
                    update_time INTEGER,
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
            upd_cur.execute(f"DELETE FROM aurora_prob WHERE update_time < {update_time}")
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} old rows deleted")
            upd_con.commit()
            logging.info("DB update finished")
        except BaseException as e:
            logging.error(f"DB update FAILED - {e}")
        finally:
            upd_con.close()


def pull_uptimerobot_data(update_time):
    uptime = [
        '/api/v1/forecast/cities (DOWN if city name is missing)',
        '/api/v1/forecast/cities (DOWN if daily forecast is an empty list)',
        '/api/v1/forecast/cities (DOWN if hourly forecast is an empty list)',
        '/api/v1/forecast/cities (DOWN if status is not 2xx or 3xx)',
        '/api/v1/forecast/cities/name (DOWN if city name is missing)',
        '/api/v1/forecast/cities/name (DOWN if daily forecast is an empty list)',
        '/api/v1/forecast/cities/name (DOWN if hourly forecast is an empty list)',
        '/api/v1/forecast/cities/name (DOWN if status is not 2xx or 3xx)',
        '/api/v1/meta (DOWN if status is not 2xx or 3xx)',
        '/api/v1/version (DOWN if status is not 2xx or 3xx)',
        '/privacy-policy (DOWN if page title is missing)',
        '/privacy-policy (DOWN if status is not 2xx or 3xx)',
        '/api/v1/metrics (DOWN if status is not 2xx or 3xx)'
    ]

    meta = {
        '/api/v1/meta (DOWN if aurora forecast is out of date)': 'aurora',
        '/api/v1/meta (DOWN if forecast download fallback has failed)': 'emergency',
        '/api/v1/meta (DOWN if forecast download has failed)': 'forecast',
        '/api/v1/meta (DOWN if forecast fields look to be missing)': 'forecast',
        '/api/v1/forecast/cities (DOWN if any forecast fields have defaulted to -999)': 'forecast',
    }

    if os.environ['UPTIMEROBOT']:
        url = "https://api.uptimerobot.com/v2/getMonitors"
        r = requests.post(
            url,
            data={
                "api_key": os.environ['UPTIMEROBOT'],
                "logs": 1
            },
            timeout=10
        )
        if r.status_code == 200:
            monit_data = json.loads(r.content)
            open(f"{data_uptimerobot_folder}uptimerobot_metrics_response.json", "wb").write(r.content)
            metrics = {k: {} for k in meta.values()}
            oldest_monit = min([e["create_datetime"] for e in monit_data["monitors"]])
            metrics["downtime"] = {oldest_monit: oldest_monit}

            for e in monit_data["monitors"]:
                is_meta = e["friendly_name"] in meta
                if is_meta:
                    # adds a bunch of duplicate stuff if multiple monits are looking at a specific meta val, shouldn't matted since they'll be 0 len though
                    metrics[meta[e["friendly_name"]]][e["create_datetime"]] = e["create_datetime"]
                if is_meta or e["friendly_name"] in uptime:
                    for ent in e["logs"]:
                        if ent["type"] == 1:
                            ek = meta[e["friendly_name"]] if is_meta and ent["duration"] > 300 and ent["reason"]["code"] != "333333" else "downtime"
                            end_dt = ent["datetime"]+ent["duration"]
                            # work out if the current incidents start time falls within a different incident, and merge them if that's the case
                            matches = {
                                k:v for k,v in metrics[ek].items()
                                if (
                                    (ent["datetime"] >= k and ent["datetime"] <= v) or # new entries start falls within existing entry
                                    (end_dt >= k and end_dt <= v) or # new entries end falls within existing entry
                                    (ent["datetime"] <= k and end_dt >= v) # old entry fully falls within the new one
                                )
                            }
                            if len(matches) > 0:
                                for k in matches.keys():
                                    del metrics[ek][k]
                                metrics[ek][min(min(matches.keys()), ent["datetime"])] = max(max(matches.values()), end_dt)
                            else:
                                metrics[ek][ent["datetime"]] = end_dt

            upd_con = sqlite3.connect(db_file)
            upd_cur = upd_con.cursor()
            try:
                upd_cur.execute("""
                    CREATE TABLE IF NOT EXISTS downtimes (
                        type TEXT,
                        start_time INTEGER,
                        duration INTEGER,
                        update_time INTEGER,
                        PRIMARY KEY (type, start_time)
                    )
                """)
                upd_cur.executemany(f"""
                    INSERT INTO downtimes (type, start_time, duration, update_time)
                    VALUES (?, ?, ?, {update_time})
                    ON CONFLICT(type, start_time) DO UPDATE SET
                        duration=excluded.duration,
                        update_time={update_time}
                """, [[ki, kj, vj-kj] for ki, vi in metrics.items() for kj, vj in vi.items()])
                logging.info(f"TABLE 'downtimes' - {upd_cur.rowcount} rows upserted")
                upd_con.commit()
                upd_cur.execute(f"DELETE FROM downtimes WHERE update_time < {update_time}")
                logging.info(f"TABLE 'downtimes' - {upd_cur.rowcount} old rows deleted")
                upd_con.commit()
                logging.info("DB update finished")
            except BaseException as e:
                logging.error(f"DB update FAILED - {e}")
            finally:
                upd_con.close()


hourly_params = {
    # is the condition code the icon?
    'conditionCode': 1, # Laika apstākļu ikona
    'airTemperature': 2, # Temperatūra
    'feelsLikeTemperature': 3, # Sajūtu temperatūra
    'windSpeed': 4, # Vēja ātrums
    'windDirection': 5, # Vēja virziens
    'windGust': 6, # Brāzmas
    'totalPrecipitation': 7, # Nokrišņi
    #8, # Atmosfēras spiediens
    #9, # Gaisa relatīvais mitrums
    # 'missing_1': 10, # UV indeks
    # 'missing_2': 11, # Pērkona varbūtība
}

daily_params = {
    #12, # Diennakts vidējais vēja virziens
    'windSpeed': [(lambda a: sum(a)/len(a), 13)], # Diennakts vidējā vēja vērtība
    'windGust': [(lambda a: max(a), 14)], # Diennakts maksimālā vēja brāzma
    'airTemperature': [
        (lambda a: max(a), 15), # Diennakts maksimālā temperatūra
        (lambda a: min(a), 16), # Diennakts minimālā temperatūra
    ],
    'totalPrecipitation': [
        (lambda a: sum(a), 17), # Diennakts nokrišņu summa
        (lambda a: sum(a), 18), # Diennakts nokrišņu varbūtība
    ],
    'conditionCode': [
        (lambda a: a, 19), # Laika apstākļu ikona nakti
        (lambda a: a, 20), # Laika apstākļu ikona diena
    ]
}

day_icons = {
    'clear': '1101',
    'partly-cloudy': '1102',
    'cloudy-with-sunny-intervals': '1103',
    'cloudy': '1104',
    'light-rain': '1503',
    'rain': '1505',
    'heavy-rain': '1504',
    'thunder': '1324',
    'isolated-thunderstorms': '1312',
    'thunderstorms': '1323',
    'heavy-rain-with-thunderstorms': '1310',
    'light-sleet': '1319',
    'sleet': '1318',
    'freezing-rain': '1317',
    'hail': '1317',
    'light-snow': '1602',
    'snow': '1601',
    'heavy-snow': '1604',
    'fog': '1401',
    'null': '0000',
}

night_icons = {
    'clear': '2101',
    'partly-cloudy': '2102',
    'cloudy-with-sunny-intervals': '2103',
    'cloudy': '2104',
    'light-rain': '2503',
    'rain': '2505',
    'heavy-rain': '2504',
    'thunder': '2324',
    'isolated-thunderstorms': '2312',
    'thunderstorms': '2323',
    'heavy-rain-with-thunderstorms': '2310',
    'light-sleet': '2319',
    'sleet': '2318',
    'freezing-rain': '2317',
    'hail': '2317',
    'light-snow': '2602',
    'snow': '2601',
    'heavy-snow': '2604',
    'fog': '2401',
    'null': '0000',
}

icon_prio = [
    'heavy-rain-with-thunderstorms',
    'thunderstorms',
    'isolated-thunderstorms',
    'thunder',
    'heavy-snow',
    'snow',
    'light-snow',
    'freezing-rain',
    'hail',
    'sleet',
    'light-sleet',
    'heavy-rain',
    'rain',
    'light-rain',
    'fog',
    'partly-cloudy',
    'cloudy',
    'cloudy-with-sunny-intervals',
    'clear',
]


def pull_lt_data(update_time):
    upd_con = sqlite3.connect(db_file)
    upd_cur = upd_con.cursor()

    place_data = None

    try:
        places = [e for e in json.loads(requests.get("https://api.meteo.lt/v1/places").content) if e['countryCode'] != "LV"]
        f_places = [[
            p['code'], # id
            'LT', # source
            p['name'], # name
            simlpify_string(p['name'].strip().lower()), # search_name
            p['coordinates']['latitude'], # lat
            p['coordinates']['longitude'], # lon
            'location_LT', # type
            p['administrativeDivision'], # county
            p['countryCode'], # country
        ] for p in places]

        upd_cur.executemany(f"""
            INSERT INTO cities (id, source, name, search_name, lat, lon, type, county, country, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {update_time})
            ON CONFLICT(id, source) DO UPDATE SET
                name=excluded.name,
                search_name=excluded.search_name,
                lat=excluded.lat,
                lon=excluded.lon,
                type=excluded.type,
                county=excluded.county,
                country=excluded.country,
                update_time={update_time}
        """, f_places)
        logging.info(f"TABLE 'cities' - LT - {upd_cur.rowcount} rows upserted")
        upd_con.commit()
        # TODO - moving deletion to its own separate step in the download process may make sense
        # initial city dl deletes this stuff before we get here
        upd_cur.execute(f"DELETE FROM cities WHERE update_time < {update_time}")
        logging.info(f"TABLE 'cities' - LT - {upd_cur.rowcount} old rows deleted")
        upd_con.commit()
        logging.info("DB update finished")

        h_params = []
        for p in places:
            place_data = json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)
            h_dates = []
            for i in range(len(place_data['forecastTimestamps'])-1):
                if int(place_data['forecastTimestamps'][i]['forecastTimeUtc'][11:13])-int(place_data['forecastTimestamps'][i+1]['forecastTimeUtc'][11:13]) in {-1, 23}:
                    h_dates.append(place_data['forecastTimestamps'][i]['forecastTimeUtc'])
                else:
                    break
            h_dates = set(h_dates)
            # d_dates = set([e['forecastTimeUtc'][:10] for e in place_data['forecastTimestamps']])

            h_params.extend([[p['code'], hourly_params[k], f['forecastTimeUtc'].replace(" ", "").replace("-", "").replace(":", "")[:12], day_icons[v] if k == 'conditionCode' else v] for f in place_data['forecastTimestamps'] for k,v in f.items() if f['forecastTimeUtc'] if h_dates and k in hourly_params])
            sleep(0.4) # trying to stay below the advertised 180 rqs / minute

        batch_size = 10000
        total = len(h_params)
        batch_count = total//batch_size
        for i in range(batch_count+1):
            upd_cur.executemany(f"""
                INSERT INTO forecast_cities (city_id, param_id, date, value, update_time)
                VALUES (?, ?, ?, ?, {update_time})
                ON CONFLICT(city_id, param_id, date) DO UPDATE SET
                    value=excluded.value,
                    update_time={update_time}
            """, h_params[i*batch_size:(i+1)*batch_size])
            logging.info(f"TABLE 'forecast_cities' - LT - {upd_cur.rowcount} rows upserted (batch {i}/{batch_count}, total {total})")
            upd_con.commit()
        # TODO - moving deletion to its own separate step in the download process may make sense
        # initial city dl deletes this stuff before we get here
        upd_cur.execute(f"DELETE FROM forecast_cities WHERE update_time < {update_time} LIMIT {batch_size}")
        logging.info(f"TABLE 'forecast_cities' - LT - {upd_cur.rowcount} old rows deleted")
        while upd_cur.rowcount > 0:
            upd_cur.execute(f"DELETE FROM forecast_cities WHERE update_time < {update_time} LIMIT {batch_size}")
            logging.info(f"TABLE 'forecast_cities' - LT - {upd_cur.rowcount} old rows deleted")
        upd_con.commit()
        logging.info("DB update finished")

    except BaseException as e:
        logging.error(f"DB update FAILED - {e}")
        logging.error(place_data)
        raise e
    finally:
        upd_con.close()


def run_downloads(datasets):
    logging.info("Triggering refresh")
    skipped_empty = False
    try:
        for ds in datasets:
            skipped_empty = download_resources(ds) or skipped_empty
    except BaseException as e:
        logging.error(f"Download failed - {e}")
        skipped_empty = True

    if skipped_empty and not os.path.isfile(run_emergency):
        logging.error("Failure encountered - setting emergency flag")
        open(run_emergency, 'w').write("")

    update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    update_db(update_time)
    update_aurora_forecast(update_time)
    pull_uptimerobot_data(update_time)
    pull_lt_data(update_time)

    if not skipped_empty:
        open(last_updated, 'w').write(
            datetime.datetime.fromtimestamp(os.path.getmtime(f"{data_folder}{target_ds[0]}.json")).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
        )
        if os.path.isfile(run_emergency):
            os.remove(run_emergency)
        if os.path.isfile(run_emergency_failed):
            os.remove(run_emergency_failed)


if __name__ == "__main__":
    logging.info("Download job starting")
    if skip_download:
        update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
        update_db(update_time)
        update_aurora_forecast(update_time)
    else:
        run_downloads(target_ds)
