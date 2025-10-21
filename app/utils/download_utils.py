import os
import json
import sqlite3
import logging
import requests

from utils import hourly_params, daily_params, simlpify_string
from settings import db_file, data_folder


logging.basicConfig(
    filename="/data/download.log",
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
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
    "DATEH": lambda r: str(r)
    .strip()
    .replace(".", "")
    .replace("-", "")
    .replace(" ", "")
    .replace(":", "")
    .replace("T", "")
    .ljust(12, "0")[:12],  # YYYYMMDDHHMM
    "CONST_LV": lambda _: "LV",
}

col_types = {
    "DATEH": "INTEGER",
    "CONST_LV": "TEXT",
    "TITLE_TEXT": "TEXT",
    "CLEANED_TEXT": "TEXT",
}

table_conf = [
    {
        "files": [
            {"name": f"{data_folder}{forecast_s}/cities.csv", "skip_if_empty": True}
        ],
        "table_name": "cities",
        "cols": [
            [
                {"name": "id", "type": "TEXT", "pk": True},
                {"name": "source", "type": "CONST_LV", "pk": True},
            ],
            [
                {"name": "name", "type": "TITLE_TEXT"},
                {"name": "search_name", "type": "CLEANED_TEXT"},
            ],
            [{"name": "lat", "type": "REAL"}],
            [{"name": "lon", "type": "REAL"}],
            [{"name": "type", "type": "TEXT"}],
            [
                {"name": "county", "type": "TEXT"},
                {"name": "country", "type": "CONST_LV"},
            ],
        ],
    },
    {
        "files": [
            {
                "name": f"{data_folder}{forecast_s}/forcity_param.csv",
                "skip_if_empty": True,
            }
        ],
        "table_name": "forecast_cities_params",
        "cols": [
            [{"name": "id", "type": "INTEGER", "pk": True}],
            [{"name": "title_lv", "type": "TEXT"}],
            [{"name": "title_en", "type": "TEXT"}],
        ],
    },
    {
        "files": [
            {
                "name": f"{data_folder}{forecast_s}/forecast_cities_day.csv",
                "skip_if_empty": True,
            },
            {
                "name": f"{data_folder}{forecast_s}/forecast_cities.csv",
                "skip_if_empty": True,
                "do_emergency_dl": True,
            },
        ],
        "table_name": "forecast_cities",
        "cols": [
            [{"name": "city_id", "type": "TEXT", "pk": True}],
            [{"name": "param_id", "type": "INTEGER", "pk": True}],
            [{"name": "date", "type": "DATEH", "pk": True}],
            [{"name": "value", "type": "REAL"}],
        ],
    },
    {
        "files": [{"name": f"{data_folder}{warning_s}/novadi.csv"}],
        "table_name": "municipalities",
        "cols": [
            [{"name": "id", "type": "INTEGER", "pk": True}],
            [{"name": "name_lv", "type": "TEXT"}],
            [{"name": "name_en", "type": "TEXT"}],
        ],
    },
    {
        "files": [{"name": f"{data_folder}{warning_s}/bridinajumu_novadi.csv"}],
        "table_name": "warnings_municipalities",
        "cols": [
            [{"name": "warning_id", "type": "INTEGER"}],
            [{"name": "municipality_id", "type": "INTEGER"}],
        ],
    },
    {
        "files": [{"name": f"{data_folder}{warning_s}/bridinajumu_poligoni.csv"}],
        "table_name": "warnings_polygons",
        "cols": [
            [{"name": "warning_id", "type": "INTEGER", "pk": True}],
            [{"name": "polygon_id", "type": "INTEGER", "pk": True}],
            [{"name": "lat", "type": "REAL"}],
            [{"name": "lon", "type": "REAL"}],
            [{"name": "order_id", "type": "INTEGER", "pk": True}],
        ],
    },
    {  # TODO: partial at the moment - finish this
        "files": [{"name": f"{data_folder}{warning_s}/bridinajumu_metadata.csv"}],
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
        ],
    },
]

lt_hourly_params = {
    # is the condition code the icon?
    "conditionCode": 1,  # Laika apstākļu ikona
    "airTemperature": 2,  # Temperatūra
    "feelsLikeTemperature": 3,  # Sajūtu temperatūra
    "windSpeed": 4,  # Vēja ātrums
    "windDirection": 5,  # Vēja virziens
    "windGust": 6,  # Brāzmas
    "totalPrecipitation": 7,  # Nokrišņi
    # 8, # Atmosfēras spiediens
    # 9, # Gaisa relatīvais mitrums
    # 'missing_1': 10, # UV indeks
    # 'missing_2': 11, # Pērkona varbūtība
}

lt_day_icons = {
    "clear": "1101",
    "partly-cloudy": "1102",
    "cloudy-with-sunny-intervals": "1103",
    "cloudy": "1104",
    "light-rain": "1503",
    "rain": "1505",
    "heavy-rain": "1504",
    "thunder": "1324",
    "isolated-thunderstorms": "1312",
    "thunderstorms": "1323",
    "heavy-rain-with-thunderstorms": "1310",
    "light-sleet": "1319",
    "sleet": "1318",
    "freezing-rain": "1317",
    "hail": "1317",
    "light-snow": "1602",
    "snow": "1601",
    "heavy-snow": "1604",
    "fog": "1401",
    "null": "0000",
}

lt_night_icons = {
    "clear": "2101",
    "partly-cloudy": "2102",
    "cloudy-with-sunny-intervals": "2103",
    "cloudy": "2104",
    "light-rain": "2503",
    "rain": "2505",
    "heavy-rain": "2504",
    "thunder": "2324",
    "isolated-thunderstorms": "2312",
    "thunderstorms": "2323",
    "heavy-rain-with-thunderstorms": "2310",
    "light-sleet": "2319",
    "sleet": "2318",
    "freezing-rain": "2317",
    "hail": "2317",
    "light-snow": "2602",
    "snow": "2601",
    "heavy-snow": "2604",
    "fog": "2401",
    "null": "0000",
}

lt_icon_prio = [
    "heavy-rain-with-thunderstorms",
    "thunderstorms",
    "isolated-thunderstorms",
    "thunder",
    "heavy-snow",
    "snow",
    "light-snow",
    "freezing-rain",
    "hail",
    "sleet",
    "light-sleet",
    "heavy-rain",
    "rain",
    "light-rain",
    "fog",
    "partly-cloudy",
    "cloudy",
    "cloudy-with-sunny-intervals",
    "clear",
]

lt_daily_params = {
    # 12, # Diennakts vidējais vēja virziens
    "windSpeed": [
        (lambda d, n: sum(d + n) / len(d + n), 13)
    ],  # Diennakts vidējā vēja vērtība
    "windGust": [(lambda d, n: max(d + n), 14)],  # Diennakts maksimālā vēja brāzma
    "airTemperature": [
        (lambda d, n: max(d + n), 15),  # Diennakts maksimālā temperatūra
        (lambda d, n: min(d + n), 16),  # Diennakts minimālā temperatūra
    ],
    "totalPrecipitation": [
        (lambda d, n: sum(d + n), 17),  # Diennakts nokrišņu summa
        # (lambda a: sum(a), 18), # Diennakts nokrišņu varbūtība
    ],
    "conditionCode": [
        (
            lambda _, n: lt_night_icons[[e for e in lt_icon_prio if e in n][0]],
            19,
        ),  # Laika apstākļu ikona nakti
        (
            lambda d, _: lt_day_icons[[e for e in lt_icon_prio if e in d][0]],
            20,
        ),  # Laika apstākļu ikona diena
    ],
}


def update_aurora_forecast(update_time):  # TODO: cleanup
    url = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
    fpath = f"{data_folder}ovation_aurora_latest.json"
    times_fpath = f"{data_folder}ovation_aurora_times.json"

    r = requests.get(url, timeout=10)
    if r.status_code == 200:
        with open(fpath, "wb") as f:
            f.write(r.content)
        aurora_data = json.loads(r.content)
        with open(times_fpath, "w") as f:
            f.write(
                json.dumps(
                    {
                        "Observation Time": aurora_data["Observation Time"],
                        "Forecast Time": aurora_data["Forecast Time"],
                    }
                )
            )

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
            upd_cur.executemany(
                f"""
                INSERT INTO aurora_prob (lon, lat, aurora, update_time)
                VALUES (?, ?, ?, {update_time})
                ON CONFLICT(lon, lat) DO UPDATE SET
                    aurora=excluded.aurora,
                    update_time={update_time}
            """,
                aurora_data["coordinates"],
            )
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} rows upserted")
            upd_con.commit()
            upd_cur.execute(
                f"DELETE FROM aurora_prob WHERE update_time < {update_time}"
            )
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} old rows deleted")
            upd_con.commit()
            logging.info("DB update finished")
        except BaseException as e:
            logging.error(f"DB update FAILED - {e}")
        finally:
            upd_con.close()
