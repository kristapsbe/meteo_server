import os
import re
import json
import pytz
import sqlite3
import logging
import pathlib
import uvicorn
import datetime

from utils import simlpify_string
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from settings import editdist_extension


# TODO: when set to True this depends on responses containing weather warnings being present on the computer
warning_mode = False
db_f = "meteo.db"
if warning_mode:
    db_f = "meteo_warning_test.db"

regex = re.compile('[^a-zA-Z āčēģīķļņšūžĀČĒĢĪĶĻŅŠŪŽ]')

con = sqlite3.connect(db_f)
con.enable_load_extension(True)
con.load_extension(editdist_extension)   

# the cursor doesn't actually do anything in sqlite3, just reusing it
# https://stackoverflow.com/questions/54395773/what-are-the-side-effects-of-reusing-a-sqlite3-cursor
cur = con.cursor()
data_f = "data/"
if warning_mode:
    data_f = "data_warnings/"

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
    # no point in even looking if we're outside of this box
    if lat < 55.6 or lat > 58.2 or lon < 20.8 or lon > 28.3:
        return ()
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
                    WHEN 'ciems' THEN 5
                END as ctype,
                ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 as distance
            FROM
                cities
            WHERE
                type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs', 'ciems')
        )
        SELECT
            id, name, ctype, distance
        FROM
            city_distances
        WHERE
            distance <= ({distance}/ctype)
        ORDER BY
            ctype ASC, distance ASC
        LIMIT 1
    """).fetchall()
    if len(cities) == 0:
        if distance < max_distance:
            return get_closest_city(cur, lat, lon, distance+5, max_distance)
        else:
            return ()
    else:
        return cities[0]
    
    
def get_city_by_name(city_name):
    cities = cur.execute(f"""
        WITH edit_distances AS (
            SELECT
                id,
                name,
                lat,
                lon,
                CASE type
                    WHEN 'republikas pilseta' THEN 1
                    WHEN 'citas pilsētas' THEN 2
                    WHEN 'rajona centrs' THEN 3
                    WHEN 'pagasta centrs' THEN 4
                    WHEN 'ciems' THEN 5
                END as ctype,
                editdist3(search_name, '{city_name}') AS distance
            FROM
                cities
            WHERE
                type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs', 'ciems')
        )
        SELECT
            id, name, lat, lon, ctype, distance
        FROM
            edit_distances
        ORDER BY
            distance ASC, ctype ASC
        LIMIT 1
    """).fetchall()
    if len(cities) == 0:
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


def get_aurora_probability(cur, lat, lon):
    aurora_probs = cur.execute(f"""
        SELECT
            aurora
        FROM
            aurora_prob
        WHERE
            lat={lat} AND lon={lon}
        LIMIT 1
    """).fetchall()
    aurora_probs_time = json.loads(open("data/ovation_aurora_times.json", "r").read())

    return {
        "prob": aurora_probs[0][0] if len(aurora_probs) > 0 else 0, # just default to 0 if there's no data
        "time": datetime.datetime.strptime(aurora_probs_time["Forecast Time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    }


def get_city_reponse(city, lat, lon, add_params, add_aurora):
    h_params = get_params(cur, hourly_params_q)
    d_params = get_params(cur, daily_params_q)
    c_date = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    if warning_mode:
        c_date = "202407270000"
    h_forecast = get_forecast(cur, city, c_date, h_params)
    d_forecast = get_forecast(cur, city, c_date, d_params)
    warnings = get_warnings(cur, lat, lon)
    metadata_f = f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam.json"
    metadata = json.loads(open(metadata_f, "r").read())

    ret_val = {
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
    if add_params:
        ret_val["hourly_params"] = [p[1:] for p in h_params]
        ret_val["daily_params"] = [p[1:] for p in d_params]

    if add_aurora:
        ret_val["aurora_probs"] = get_aurora_probability(cur, round(lat), round(lon))
    return ret_val


# http://localhost:8000/api/v1/forecast/cities?lat=56.9730&lon=24.1327
@app.get("/api/v1/forecast/cities")
async def get_city_forecasts(lat: float, lon: float, add_params: bool = True, add_aurora: bool = False):
    city = get_closest_city(cur, lat, lon, 10, 80) # TODO revisit starting dist
    return get_city_reponse(city, lat, lon, add_params, add_aurora)


# http://localhost:8000/api/v1/forecast/cities/name?city_name=vamier
@app.get("/api/v1/forecast/cities/name")
async def get_city_forecasts(city_name: str, add_params: bool = True, add_aurora: bool = False):
    city = get_city_by_name(simlpify_string(regex.sub('', city_name).strip().lower()))
    return get_city_reponse(city, city[2] if len(city) > 0 else None, city[3] if len(city) > 0 else None, add_params, add_aurora)


# http://localhost:8000/privacy-policy
@app.get("/privacy-policy", response_class=HTMLResponse)
async def get_version(lang: str = "en"):
    match lang:
        case "lv":
            return open("privatuma-politika.html").read()
        case _:
            return open("privacy-policy.html").read()


if __name__ == "__main__":
    cwd = pathlib.Path(__file__).parent.resolve()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_config=f"{cwd}/log.ini")
