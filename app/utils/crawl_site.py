# ONLY IN CASE OF EMERGENCIES
# looks like the upload to the open data portal can blow up
# manually trigger this to fetch hourly forecasts from the LVĢMC website instead
#
# I'll just keep reusing city and param data, and the LVĢMC site appears to use he same data
# as they upload to the open data porta when it comes to daily forecasts -
# meaning that I can't use the site as a fallback there
import os
import json
import pytz
import time
import sqlite3
import logging
import datetime
import requests

from settings import db_file, data_folder, run_emergency


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

if not os.path.isfile(run_emergency):
    logging.info("No emergency - exiting")
    exit()

# TODO cleanup - copied from download script atm
url = 'https://videscentrs.lvgmc.lv/data/weather_forecast_for_location_hourly?punkts='

con = sqlite3.connect(db_file)
cur = con.cursor()

ids = [e[0] for e in cur.execute("""
    SELECT
        id
    FROM
        cities
    WHERE
        type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs')
""").fetchall()]

ct = 1
total = len(ids)
csv = ['CITY_ID,PARA_ID,DATUMS,VERTIBA']
for id in ids:
    print(f"pulling {id} ({ct}/{total})")
    ct += 1

    rs = requests.get(f"{url}{id}", timeout=10)
    data = json.loads(rs.content)
    for e in data:
        l = e['laiks']
        datestring = f"{l[:4]}-{l[4:6]}-{l[6:8]} {l[8:10]}:{l[10:12]}:00"
        csv.append(f'"{id}","1","{datestring}","{e["laika_apstaklu_ikona"]}"')
        csv.append(f'"{id}","2","{datestring}","{e["temperatura"]}"')
        csv.append(f'"{id}","3","{datestring}","{e["sajutu_temperatura"]}"')
        csv.append(f'"{id}","4","{datestring}","{e["veja_atrums"]}"')
        csv.append(f'"{id}","5","{datestring}","{e["veja_virziens"]}"')
        csv.append(f'"{id}","6","{datestring}","{e["brazmas"]}"')
        csv.append(f'"{id}","7","{datestring}","{e["nokrisni_1h"]}"')
        csv.append(f'"{id}","10","{datestring}","{e["uvi_indekss"] if e["uvi_indekss"] is not None else 0}"')
        csv.append(f'"{id}","11","{datestring}","{e["perkons"]}"')
    time.sleep(0.5) # don't want to spam the site too much


if len(csv) > 1:
    with open(f"{data_folder}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv", 'w') as f:
        f.write('\n'.join(csv))
    open(run_emergency, 'w').write(datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"))
    logging.info("Successfully performed emergency download")
else:
    os.remove(run_emergency)
    logging.info("Failed to perform emergency download")
