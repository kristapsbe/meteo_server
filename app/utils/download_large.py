import json
import pytz
import sqlite3
import logging
import datetime
import requests

from time import sleep
from utils import simlpify_string
from settings import db_file
from download_utils import lt_hourly_params, lt_daily_params, lt_day_icons, MIN_PARAM_COUNT, target_ds
from download_small import do_20_m_download, target_ds
from download_aurora import do_aurora_download


logging.basicConfig(
    filename='/data/download.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


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

        for p in places:
            place_data = json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)
            h_dates = []
            for i in range(len(place_data['forecastTimestamps'])-1):
                if int(place_data['forecastTimestamps'][i]['forecastTimeUtc'][11:13])-int(place_data['forecastTimestamps'][i+1]['forecastTimeUtc'][11:13]) in {-1, 23}:
                    h_dates.append(place_data['forecastTimestamps'][i]['forecastTimeUtc'])
                else:
                    break
            h_dates = set(h_dates)
            d_dates = set([e['forecastTimeUtc'][:10] for e in place_data['forecastTimestamps']])

            params = [[p['code'], lt_hourly_params[k], f['forecastTimeUtc'].replace(" ", "").replace("-", "").replace(":", "")[:12], lt_day_icons[v] if k == 'conditionCode' else v] for f in place_data['forecastTimestamps'] for k,v in f.items() if f['forecastTimeUtc'] in h_dates and k in lt_hourly_params]

            sorted_d_dates = sorted(list(d_dates))
            # print(sorted_d_dates)
            for i in range(1, len(sorted_d_dates)):
                tmp_day = [e for e in place_data['forecastTimestamps'] if e['forecastTimeUtc'] >= f"{sorted_d_dates[i-1][:10]} 09:00:00" and e['forecastTimeUtc'] < f"{sorted_d_dates[i-1][:10]} 21:00:00"]
                tmp_night = [e for e in place_data['forecastTimestamps'] if e['forecastTimeUtc'] >= f"{sorted_d_dates[i-1][:10]} 21:00:00" and e['forecastTimeUtc'] < f"{sorted_d_dates[i][:10]} 09:00:00"]
                for k in tmp_day[0].keys():
                    if k in lt_daily_params:
                        for f in lt_daily_params[k]:
                            params.append([p['code'], f[1], f"{tmp_day[0]['forecastTimeUtc'].replace(' ', '').replace('-', '').replace(':', '')[:8]}0000", f[0]([e[k] for e in tmp_day], [e[k] for e in tmp_night])])

            sleep(0.4) # trying to stay below the advertised 180 rqs / minute

            batch_size = 10000
            total = len(params)
            batch_count = total//batch_size
            for i in range(batch_count+1):
                upd_cur.executemany(f"""
                    INSERT INTO forecast_cities (city_id, param_id, date, value, update_time)
                    VALUES (?, ?, ?, ?, {update_time})
                    ON CONFLICT(city_id, param_id, date) DO UPDATE SET
                        value=excluded.value,
                        update_time={update_time}
                """, params[i*batch_size:(i+1)*batch_size])
                logging.info(f"TABLE 'forecast_cities' - LT - {upd_cur.rowcount} rows upserted (batch {i}/{batch_count}, total {total})")
                upd_con.commit()

        # upd_cur.execute(f"DELETE FROM forecast_cities WHERE update_time < {update_time}")
        # logging.info(f"TABLE 'forecast_cities' - LT - {upd_cur.rowcount} old rows deleted")
        logging.info(f"TABLE 'forecast_cities' - LT - deletion currently disabled")
        upd_con.commit()
        logging.info("DB update finished")

    except BaseException as e:
        logging.error(f"DB update FAILED - {e}")
        logging.error(place_data)
        raise e
    finally:
        upd_con.close()


def do_4_h_download(update_time):
    pull_lt_data(update_time)


if __name__ == "__main__":
    logging.info("Download job starting")
    do_aurora_download()

    update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    do_20_m_download(target_ds, update_time)
    do_4_h_download(update_time) # TODO: make emergency dl part of this?
