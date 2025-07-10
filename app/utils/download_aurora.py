import json
import pytz
import sqlite3
import logging
import datetime
import requests

from settings import db_file, data_folder


skip_download = False

logging.basicConfig(
    filename='/data/download.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


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
            upd_cur.execute(f"DELETE FROM aurora_prob WHERE update_time < {update_time}")
            logging.info(f"TABLE 'aurora_prob' - {upd_cur.rowcount} old rows deleted")
            upd_con.commit()
            logging.info("DB update finished")
        except BaseException as e:
            logging.error(f"DB update FAILED - {e}")
        finally:
            upd_con.close()


if __name__ == "__main__":
    logging.info("Aurora download job starting")
    update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    update_aurora_forecast(update_time)
