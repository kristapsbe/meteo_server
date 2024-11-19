import os
import re
import json
import pytz
import sqlite3
import logging
import pathlib
import uvicorn
import datetime

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from utils import simlpify_string
from settings import editdist_extension, db_file, data_folder, warning_mode


last_updated = 'last_updated'
if not os.path.isfile(last_updated):
    open(last_updated, 'w').write("197001010000")

regex = re.compile('[^a-zA-Z āčēģīķļņšūžĀČĒĢĪĶĻŅŠŪŽ]')

con = sqlite3.connect(db_file)
con.enable_load_extension(True)
con.load_extension(editdist_extension)

# the cursor doesn't actually do anything in sqlite3, just reusing it
# https://stackoverflow.com/questions/54395773/what-are-the-side-effects-of-reusing-a-sqlite3-cursor
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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

hourly_params = "','".join([
    'Laika apstākļu piktogramma',
    'Temperatūra (°C)',
    'Sajūtu temperatūra (°C)',
    'Vēja ātrums (m/s)',
    'Vēja virziens (°)',
    'Brāzmas (m/s)',
    'Nokrišņi (mm)',
    'UV indekss (0-10)',
    'Pērkona negaisa varbūtība (%)',
])

daily_params = "','".join([
    'Diennakts vidējā vēja vērtība (m/s)',
    'Diennakts maksimālā vēja brāzma (m/s)',
    'Diennakts maksimālā temperatūra (°C)',
    'Diennakts minimālā temperatūra (°C)',
    'Diennakts nokrišņu summa (mm)',
    'Diennakts nokrišņu varbūtība (%)',
    'Laika apstākļu piktogramma nakti',
    'Laika apstākļu piktogramma diena',
])


def get_params(cur, param_q):
    return cur.execute(f"""
        SELECT
            id, title_lv, title_en
        FROM
            forecast_cities_params
        WHERE
            title_lv in ('{param_q}')
    """).fetchall()


def is_emergency():
    return os.path.isfile('run_emergency')


def get_location_range(force_all=False):
    if force_all or not is_emergency():
        return "('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs', 'ciems')"
    else:
        return "('republikas pilseta', 'citas pilsētas', 'rajona centrs')"


def get_closest_city(cur, lat, lon, distance=10, force_all=False, only_closest=False):
    cities = []
    only_closest_active = lat < 55.7 or lat > 58.05 or lon < 20.95 or lon > 28.25 or only_closest
    where_str = f"""
        WHERE
            distance <= ({distance}/ctype)
    """
    if only_closest_active:
        where_str = ""

    # calculating dist in km since using Euclidean doesn't appear to yield big savings
    # and this makes messing around with distance values a bit more intuitive
    cities = cur.execute(f"""
        WITH city_distances AS (
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
                ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 as distance
            FROM
                cities
            WHERE
                type in {get_location_range(force_all)}
        )
        SELECT
            id, name, lat, lon, ctype, distance
        FROM
            city_distances
        {where_str}
        ORDER BY
            ctype ASC, distance ASC
        LIMIT 1
    """).fetchall()

    if len(cities) == 0:
        if only_closest_active:
            return ()
        else:
            return get_closest_city(cur, lat, lon, distance, force_all, only_closest=True)
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
                type in {get_location_range(True)}
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
    # since I'm using squares anyhow precomputing them to save time
    relevant_warnings = cur.execute(f"""
        SELECT
            warning_id
        FROM
            warning_bounds
        WHERE
            {lat} >= min_lat AND {lat} <= max_lat AND {lon} >= min_lon AND {lon} <= max_lon
    """).fetchall()
    warnings = []
    if len(relevant_warnings) > 0:
        # the weather service occasionally serves the same warnings
        # for the same are, and with the same text, but with two
        # different intensity levels - getting only the highest intensity
        warnings = cur.execute(f"""
            WITH warning_levels AS (
                SELECT DISTINCT
                    id,
                    CASE intensity_en
                        WHEN 'Red' THEN 3
                        WHEN 'Orange' THEN 2
                        WHEN 'Yellow' THEN 1
                    END as intensity_level,
                    type_lv,
                    description_lv
                FROM
                    warnings
                WHERE
                    id in ({", ".join([str(w[0]) for w in relevant_warnings])})
            ),
            warnings_unique_texts AS (
                SELECT DISTINCT
                    max(intensity_level) AS max_intensity,
                    type_lv,
                    description_lv
                FROM
                    warning_levels
                GROUP BY
                    type_lv,
                    description_lv
            ),
            warning_filtered_ids AS (
                SELECT DISTINCT
                    id
                FROM
                    warning_levels AS wl INNER JOIN warnings_unique_texts AS wt ON
                        wl.intensity_level = wt.max_intensity
                        AND wl.type_lv = wt.type_lv
                        AND wl.description_lv = wt.description_lv
            ),
            warnings_raw AS (
                SELECT DISTINCT
                    id,
                    intensity_lv,
                    intensity_en,
                    CASE intensity_en
                        WHEN 'Red' THEN 3
                        WHEN 'Orange' THEN 2
                        WHEN 'Yellow' THEN 1
                    END as intensity_val,
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
                    id in warning_filtered_ids
            )
            SELECT
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
                warnings_raw
            ORDER BY
                intensity_val DESC
        """).fetchall()
    return warnings


def get_simple_warnings(cur, lat, lon):
    # TODO: turning the warning polygons into big squares - this should at least work - should use the actual poly bounds at some point
    # since I'm using squares anyhow precomputing them to save time
    relevant_warnings = cur.execute(f"""
        SELECT
            warning_id
        FROM
            warning_bounds
        WHERE
            {lat} >= min_lat AND {lat} <= max_lat AND {lon} >= min_lon AND {lon} <= max_lon
    """).fetchall()
    warnings = []
    if len(relevant_warnings) > 0:
        # the weather service occasionally serves the same warnings
        # for the same are, and with the same text, but with two
        # different intensity levels - getting only the highest intensity
        warnings = cur.execute(f"""
            WITH warning_levels AS (
                SELECT DISTINCT
                    id,
                    CASE intensity_en
                        WHEN 'Red' THEN 3
                        WHEN 'Orange' THEN 2
                        WHEN 'Yellow' THEN 1
                    END as intensity_level,
                    type_lv,
                    description_lv
                FROM
                    warnings
                WHERE
                    id in ({", ".join([str(w[0]) for w in relevant_warnings])})
            ),
            warnings_unique_texts AS (
                SELECT DISTINCT
                    max(intensity_level) AS max_intensity,
                    type_lv,
                    description_lv
                FROM
                    warning_levels
                GROUP BY
                    type_lv,
                    description_lv
            ),
            warning_filtered_ids AS (
                SELECT DISTINCT
                    id
                FROM
                    warning_levels AS wl INNER JOIN warnings_unique_texts AS wt ON
                        wl.intensity_level = wt.max_intensity
                        AND wl.type_lv = wt.type_lv
                        AND wl.description_lv = wt.description_lv
            ),
            warnings_raw AS (
                SELECT DISTINCT
                    id,
                    type_lv,
                    type_en,
                    intensity_lv,
                    intensity_en,
                    CASE intensity_en
                        WHEN 'Red' THEN 3
                        WHEN 'Orange' THEN 2
                        WHEN 'Yellow' THEN 1
                    END as intensity_val,
                    description_lv,
                    description_en
                FROM
                    warnings
                WHERE
                    id in warning_filtered_ids
            )
            SELECT
                id,
                type_lv,
                type_en,
                intensity_lv,
                intensity_en,
                description_lv,
                description_en
            FROM
                warnings_raw
            ORDER BY
                intensity_val DESC
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


# TODO: rip out the params that are no longer needed
def get_city_reponse(city, add_last_no_skip, h_city_override, use_simple_warnings, add_city_coords):
    lat = lon = 0.0
    if len(city) > 0:
        lat = float(city[2])
        lon = float(city[3])

    h_params = get_params(cur, hourly_params)
    d_params = get_params(cur, daily_params)
    c_date = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    if warning_mode:
        c_date = "202407270000"
    h_forecast = get_forecast(cur, city if h_city_override is None else h_city_override, c_date, h_params)
    d_forecast = get_forecast(cur, city, c_date, d_params)
    metadata_f = f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam.json"
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
        "aurora_probs": get_aurora_probability(cur, round(lat), round(lon)),
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
        # TODO: get local timezone instead
        "last_downloaded": datetime.datetime.fromtimestamp(os.path.getmtime(metadata_f)).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"),
    }

    if add_city_coords:
        ret_val["lat"] = lat
        ret_val["lon"] = lon

    if use_simple_warnings:
        warnings = get_simple_warnings(cur, lat, lon)
        tmp_warnings = {}
        for w in warnings:
            tmp_key = f"{w[1]}:{w[3]}" # type and intensity
            if tmp_key not in tmp_warnings:
                tmp_warnings[tmp_key] = {
                    "ids": [w[0]],
                    "type": w[1:3],
                    "intensity": w[3:5],
                    "description_lv": [w[5]],
                    "description_en": [w[6]],
                }
            else:
                tmp_warnings[tmp_key]["ids"].append(w[0])
                tmp_warnings[tmp_key]["description_lv"].append(w[5])
                tmp_warnings[tmp_key]["description_en"].append(w[6])
        ret_val["warnings"] = list(tmp_warnings.values())
    else:
        # TODO: get rid of this once noone's using it
        warnings = get_warnings(cur, lat, lon)
        ret_val["warnings"] = [{
            "id": w[0],
            "intensity": w[1:3],
            "regions": w[3:5],
            "type": w[5:7],
            "time": w[7:9],
            "description": w[9:]
        } for w in warnings]

    if add_last_no_skip:
        ret_val["last_downloaded_no_skip"] = open(last_updated, 'r').readline().strip()
    return ret_val


# http://localhost:8000/api/v1/forecast/cities?lat=56.9730&lon=24.1327
@app.get("/api/v1/forecast/cities")
async def get_city_forecasts(lat: float, lon: float, add_last_no_skip: bool = False, use_simple_warnings: bool = False, add_city_coords=False):
    city = get_closest_city(cur=cur, lat=lat, lon=lon, force_all=True)
    # TODO: test more carefully
    h_city_override = None
    if is_emergency() and len(city) > 0:
        h_city_override = get_closest_city(cur=cur, lat=city[2], lon=city[3])
    return get_city_reponse(city, add_last_no_skip, h_city_override, use_simple_warnings, add_city_coords)


# http://localhost:8000/api/v1/forecast/cities/name?city_name=vamier
@app.get("/api/v1/forecast/cities/name")
async def get_city_forecasts_name(city_name: str, add_last_no_skip: bool = False, use_simple_warnings: bool = False, add_city_coords=False):
    city = get_city_by_name(simlpify_string(regex.sub('', city_name).strip().lower()))
    # TODO: test more carefully
    h_city_override = None
    if is_emergency() and len(city) > 0:
        h_city_override = get_closest_city(cur=cur, lat=city[2], lon=city[3])
    return get_city_reponse(city, add_last_no_skip, h_city_override, use_simple_warnings, add_city_coords)


# http://localhost:8000/privacy-policy
@app.get("/privacy-policy", response_class=HTMLResponse)
async def get_privacy_policy(lang: str = "en"):
    match lang:
        case "lv":
            return open("privacy_policy/privatuma-politika.html").read()
        case _:
            return open("privacy_policy/privacy-policy.html").read()


# http://localhost:8000/api/v1/meta
@app.get("/api/v1/meta")
async def get_meta():
    retval = {
        "is_emergency": is_emergency()
    }
    if retval["is_emergency"]:
        retval["emergency_dl"] = open('run_emergency', 'r').readline()
    return retval


if __name__ == "__main__":
    cwd = pathlib.Path(__file__).parent.resolve()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_config=f"{cwd}/log.ini")
