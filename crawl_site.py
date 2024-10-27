# ONLY IN CASE OF EMERGENCIES
# looks like the upload to the open data portal can blow up 
# manually trigger this to fetch hourly forecasts from the LVĢMC website instead
#
# TODO: looks like more than just the hourly forecasts can go bad
# set up rescue jobs for the rest of the tables
# NOTE: I don't think I actually need this thing to update the table - I can just let the download script do it
# TODO: make individual rescue functions so that I don't overwrite good data
import os
import json
import pytz
import time
import sqlite3
import logging
import datetime
import requests


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

if not os.path.isfile('run_emergency'):
    logging.info(f"No emergency - exiting")
    exit()

# TODO cleanup - copied from download script atm
db_f = "meteo.db"
data_f = "data/"
url = 'https://videscentrs.lvgmc.lv/data/weather_forecast_for_location_hourly?punkts='

con = sqlite3.connect(db_f)
cur = con.cursor()

# TODO: should I make the server more aware of the fact that this script has been run?
# that would mean that I could just pull the republic cities + some selected locations as previously planned
ids = [e[0] for e in cur.execute("""
    SELECT
        id
    FROM
        cities
    WHERE
        type in ('republikas pilseta', 'citas pilsētas', 'rajona centrs')
""").fetchall()]

ids = sorted(ids, key=lambda i: int(i[1:])) # start with lower ids in case we blow up

ct = 1
total = len(ids)
csv = ['CITY_ID,PARA_ID,DATUMS,VERTIBA']
for id in ids:
    print(f"pulling {id} ({ct}/{total})")
    ct += 1

    rs = requests.get(f"{url}{id}")
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
    with open(f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv", 'w') as f:
        f.write('\n'.join(csv))
    open('run_emergency', 'w').write(datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M"))
    logging.info("Successfully performed emergency download")
else:
    os.remove('run_emergency') 
    logging.info("Failed to perform emergency download")
