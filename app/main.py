import os
import re
import json
import pytz
import time
import sqlite3
import logging
import pathlib
import uvicorn
import datetime

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from utils.utils import simlpify_string, hourly_params, daily_params
from utils.settings import editdist_extension, db_file, data_folder, last_updated, run_emergency, run_emergency_failed


if not os.path.isfile(last_updated):
    open(last_updated, 'w').write("197001010000")

regex = re.compile('[^a-zA-Z āčēģīķļņšūžĀČĒĢĪĶĻŅŠŪŽ]')

con = sqlite3.connect(f"{db_file}", timeout=5)
con.enable_load_extension(True)
con.load_extension(".".join(editdist_extension.split(".")[:-1])) # getting rid of extension

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


MIN_PARAM_COUNT = min(len(hourly_params), len(daily_params))


def is_emergency():
    return os.path.isfile(run_emergency)


def is_param_missing():
    return cur.execute("SELECT COUNT(*) FROM missing_params").fetchall()[0][0] > 0


def has_emergency_failed():
    return os.path.isfile(run_emergency_failed)


def get_location_range(force_all=False, add_lt=False):
    if add_lt:
        return "('pilsēta', 'ciems', 'cits', 'location_LT')"
    else:
        if force_all or not is_emergency():
            return "('pilsēta', 'ciems', 'cits')"
        else:
            return "('pilsēta')"


def get_closest_city(cur, lat, lon, distance=7, force_all=False, only_closest=False, ignore_missing_params=True, add_lt=False):
    cities = []
    only_closest_active = lat < 55.7 or lat > 58.05 or lon < 20.95 or lon > 28.25 or only_closest
    where_order_str = f"""
        WHERE
            distance <= {distance}
        ORDER BY
            ctype ASC, distance ASC
    """
    if only_closest_active:
        where_order_str = """
            ORDER BY
                distance ASC
        """

    # calculating dist in km since using Euclidean doesn't appear to yield big performance savings
    # and this makes messing around with distance values a bit more intuitive
    cities = cur.execute(f"""
        WITH city_distances AS (
            SELECT
                id,
                name,
                lat,
                lon,
                CASE type
                    WHEN 'pilseta' THEN 1
                    WHEN 'ciems' THEN 2
                    WHEN 'cits' THEN 3
                    WHEN 'location_LT' THEN 4
                END as ctype,
                ACOS((SIN(RADIANS(lat))*SIN(RADIANS({lat})))+(COS(RADIANS(lat))*COS(RADIANS({lat})))*(COS(RADIANS({lon})-RADIANS(lon))))*6371 as distance
            FROM
                cities
            WHERE
                type IN {get_location_range(force_all, add_lt)}
                { "" if ignore_missing_params else " AND id NOT IN (SELECT city_id FROM missing_params)" }
        )
        SELECT
            id, name, lat, lon, ctype, distance
        FROM
            city_distances
        {where_order_str}
        LIMIT 1
    """).fetchall()

    if len(cities) == 0:
        if only_closest_active:
            return ()
        else:
            return get_closest_city(cur, lat, lon, distance, force_all, only_closest=True, ignore_missing_params=ignore_missing_params, add_lt=add_lt)
    return cities[0]


def get_city_by_name(city_name, add_lt=False):
    cities = cur.execute(f"""
        WITH edit_distances AS (
            SELECT
                id,
                name,
                lat,
                lon,
                CASE type
                    WHEN 'pilseta' THEN 1
                    WHEN 'ciems' THEN 2
                    WHEN 'cits' THEN 3
                    WHEN 'location_LT' THEN 4
                END as ctype,
                fuzzy_editdist(search_name, '{city_name}') AS distance
            FROM
                cities
            WHERE
                type in {get_location_range(True, add_lt)}
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
    # pivoting the table and using max to discard nulls
    return cur.execute(f"""
        SELECT
            city_id, date,
            {",".join([f"IFNULL(MAX(case when param_id={p} then value end), -999) AS val_{p}" for p in params])}
        FROM
            forecast_cities AS fc
        WHERE
            city_id = '{city[0]}' AND date >= '{c_date}' AND param_id IN ({",".join([str(p) for p in params])})
        GROUP BY
            city_id, date
    """).fetchall()


def get_warnings(cur, lat, lon):
    # TODO: turning the warning polygons into big squares - this should at least work - should use the actual poly bounds at some point
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
        # for the same area, and with the same text, but with two
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
        # for the same area, and with the same text, but with two
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
    aurora_probs_time = datetime.datetime.strptime(json.loads(open(f"{data_folder}/ovation_aurora_times.json", "r").read())["Forecast Time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga'))
    curr_date = datetime.datetime.now(pytz.timezone('Europe/Riga'))

    return {
        "prob": aurora_probs[0][0] if len(aurora_probs) > 0 and aurora_probs_time >= curr_date else 0, # just default to 0 if there's no data
        "time": aurora_probs_time.strftime("%Y%m%d%H%M")
    }


# TODO: delete the params that are no longer needed
def get_city_response(city, add_last_no_skip, h_city, use_simple_warnings, add_city_coords):
    lat = lon = 0.0
    if len(city) > 0:
        lat = float(city[2])
        lon = float(city[3])

    c_date = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    h_forecast = get_forecast(cur, h_city, c_date, hourly_params)
    d_forecast = get_forecast(cur, city, c_date, daily_params)
    metadata_f = f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam-jaunaka-datu-kopa.json"
    metadata = json.loads(open(metadata_f, "r").read())

    ret_val = {
        "city": str(city[1]) if len(city) > 0 else "",
        "hourly_forecast": [{
            "time": f[1],
            "vals": f[2:]
        } for f in h_forecast],
        "daily_forecast": [{
            "time": f[1] if str(f[1])[-4:] != "2300" else int((datetime.datetime.strptime(str(f[1]), "%Y%m%d%H%M") + datetime.timedelta(hours=1)).strftime("%Y%m%d%H%M")), # dealing with the clock getting turned forward
            "vals": f[2:]
        } for f in d_forecast],
        "aurora_probs": get_aurora_probability(cur, round(lat), round(lon)),
        "last_updated": metadata["result"]["metadata_modified"].replace("-", "").replace("T", "").replace(":", "")[:12],
        # TODO: get local timezone instead - at the moment I just assume that everyone's in Latvia (could also use UTC and use decides timezone in the app)
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


# http://localhost:443/api/v1/forecast/cities?lat=56.9730&lon=24.1327
@app.get("/api/v1/forecast/cities")
@app.head("/api/v1/forecast/cities") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_city_forecasts(lat: float, lon: float, add_last_no_skip: bool = False, use_simple_warnings: bool = False, add_city_coords: bool = False, enable_experimental: bool = False):
    city = get_closest_city(cur=cur, lat=lat, lon=lon, force_all=True, add_lt=enable_experimental) # always getting closest city since override only affects hourly forecasts
    return get_city_response(
        city,
        add_last_no_skip,
        get_closest_city(cur=cur, lat=city[2], lon=city[3], ignore_missing_params=False, add_lt=enable_experimental) if (is_emergency() or is_param_missing()) and len(city) > 0 else city, # getting hourly forecast for closest large city if we're in emergency mode
        use_simple_warnings,
        add_city_coords
    )


# http://localhost:443/api/v1/forecast/cities/name?city_name=vamier
@app.get("/api/v1/forecast/cities/name")
@app.head("/api/v1/forecast/cities/name") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_city_forecasts_name(city_name: str, add_last_no_skip: bool = False, use_simple_warnings: bool = False, add_city_coords: bool = False, enable_experimental: bool = False):
    city = get_city_by_name(simlpify_string(regex.sub('', city_name).strip().lower()), add_lt=enable_experimental) # always getting closest city since override only affects hourly forecasts
    return get_city_response(
        city,
        add_last_no_skip,
        get_closest_city(cur=cur, lat=city[2], lon=city[3], ignore_missing_params=False, add_lt=enable_experimental) if (is_emergency() or is_param_missing()) and len(city) > 0 else city, # getting hourly forecast for closest large city if we're in emergency mode
        use_simple_warnings,
        add_city_coords
    )


# http://localhost:443/privacy-policy
@app.get("/privacy-policy", response_class=HTMLResponse)
@app.head("/privacy-policy") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_privacy_policy(lang: str = "en"):
    match lang:
        case "lv":
            return open("html/privatuma-politika.html").read()
        case _:
            return open("html/privacy-policy.html").read()


# http://localhost:443/attribution
@app.get("/attribution", response_class=HTMLResponse)
@app.head("/attribution") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_privacy_attribution(lang: str = "en"):
    match lang:
        case "lv":
            return open("html/atribucija.html").read()
        case _:
            return open("html/attribution.html").read()


# http://localhost:443/api/v1/meta
@app.get("/api/v1/meta")
@app.head("/api/v1/meta") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_meta():
    aurora_probs_time = datetime.datetime.strptime(json.loads(open(f"{data_folder}/ovation_aurora_times.json", "r").read())["Forecast Time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga'))
    curr_date = datetime.datetime.now(pytz.timezone('Europe/Riga'))

    retval = {
        "is_emergency": is_emergency(),
        "is_param_missing": is_param_missing(),
        "is_aurora_ood": (aurora_probs_time < curr_date)
    }
    if retval["is_emergency"]:
        retval["emergency_dl"] = open(run_emergency, 'r').readline()
        retval["has_emergency_failed"] = has_emergency_failed()
    return retval


# http://localhost:443/api/v1/version
@app.get("/api/v1/version")
@app.head("/api/v1/version") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_version():
    return {
        "version": open("version.txt", "r").read().strip(),
        "updated": datetime.datetime.fromtimestamp(os.path.getmtime("version.txt")).replace(tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"),
    }


# http://localhost:443/api/v1/metrics
@app.get("/api/v1/metrics")
@app.head("/api/v1/metrics") # added for https://stats.uptimerobot.com/EAWZfpoMkw
async def get_metrics():
    now = int(time.time())
    uptimes = cur.execute(f"""
        SELECT
            type,
            (1.0-1.0*SUM(duration)/({now}-MIN(start_time)))*100 AS total,
            (1.0-1.0*SUM(CASE
                WHEN start_time >= ({now}-(60*60*24*90)) THEN duration
                WHEN start_time < ({now}-(60*60*24*90)) AND start_time+duration >= ({now}-(60*60*24*90)) THEN duration-({now}-(60*60*24*90)-start_time)
                ELSE 0
            END)/MIN((60*60*24*90), ({now}-MIN(start_time))))*100 AS ninety,
            (1.0-1.0*SUM(CASE
                WHEN start_time >= ({now}-(60*60*24*30)) THEN duration
                WHEN start_time < ({now}-(60*60*24*30)) AND start_time+duration >= ({now}-(60*60*24*30)) THEN duration-({now}-(60*60*24*30)-start_time)
                ELSE 0
            END)/MIN((60*60*24*30), ({now}-MIN(start_time))))*100 AS thirty,
            (1.0-1.0*SUM(CASE
                WHEN start_time >= ({now}-(60*60*24*7)) THEN duration
                WHEN start_time < ({now}-(60*60*24*7)) AND start_time+duration >= ({now}-(60*60*24*7)) THEN duration-({now}-(60*60*24*7)-start_time)
                ELSE 0
            END)/MIN((60*60*24*7), ({now}-MIN(start_time))))*100 AS seven,
            (1.0-1.0*SUM(CASE
                WHEN start_time >= ({now}-(60*60*24)) THEN duration
                WHEN start_time < ({now}-(60*60*24)) AND start_time+duration >= ({now}-(60*60*24)) THEN duration-({now}-(60*60*24)-start_time)
                ELSE 0
            END)/MIN((60*60*24), ({now}-MIN(start_time))))*100 AS one
        FROM
            downtimes
        GROUP BY
            type
    """).fetchall()
    ret = {
        "dashboard": "https://stats.uptimerobot.com/EAWZfpoMkw",
        "uptime": {},
        "download": {}
    }
    for e in uptimes:
        if e[0] == "downtime":
            ret["uptime"] = {
                "total": e[1],
                "90d": e[2],
                "30d": e[3],
                "7d": e[4],
                "1d": e[5]
            }
        else:
            ret["download"][e[0]] = {
                "total": e[1],
                "90d": e[2],
                "30d": e[3],
                "7d": e[4],
                "1d": e[5]
            }
    return ret


if __name__ == "__main__":
    cwd = pathlib.Path(__file__).parent.resolve()
    uvicorn.run(app, host="app", port=8000, log_config=f"{cwd}/log.ini")
