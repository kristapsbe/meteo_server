# ONLY IN CASE OF EMERGENCIES
# looks like the upload to the open data portal can blow up 
# manually trigger this to fetch hourly forecasts from the LVĢMC website instead
import os
import json
import time
import pandas as pd
import sqlite3
import logging
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
        type in ('republikas pilseta', 'citas pilsētas')
""").fetchall()]

ids = sorted(ids, key=lambda i: int(i[1:])) # start with lower ids in case we blow up

ct = 0
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


with open('data/meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities_crawled.csv', 'w') as f:
    f.write('\n'.join(csv))


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

table_conf = [{ # only updating the forecasts and assuming I have the dailies
    "files": [
        f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities_day.csv",
        f"{data_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities_crawled.csv"
    ],
    "table_name": "forecast_cities",
    "cols": [
        [{"name": "city_id", "type": "TEXT", "pk": True}],
        [{"name": "param_id", "type": "INTEGER", "pk": True}],
        [{"name": "date", "type": "DATEH", "pk": True}],
        [{"name": "value", "type": "REAL"}],
    ]
}]

def update_table(t_conf, db_cur):
    logging.info(f"UPDATING '{t_conf["table_name"]}'")
    df = None
    for data_file in t_conf["files"]:
        tmp_df = pd.read_csv(data_file).dropna()
        for ct in range(len(t_conf["cols"])):
            for col in t_conf["cols"][ct]:
                tmp_df[f"_new_{col["name"]}"] = tmp_df[tmp_df.columns[ct]].apply(col_parsers[col["type"]])
        tmp_df = tmp_df[[f"_new_{c["name"]}" for cols in t_conf["cols"] for c in cols]]
        if df is None:
            df = pd.DataFrame(tmp_df)
        else:
            df = pd.concat([df, tmp_df])

    # TODO: FIX
    df = df.groupby(['_new_city_id', '_new_param_id', '_new_date']).max().reset_index() # getting rid of duplicates

    pks = [c["name"] for cols in t_conf["cols"] for c in cols if c.get("pk", False)]
    primary_key_q = "" if len(pks) < 1 else f", PRIMARY KEY ({", ".join(pks)})"
    db_cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t_conf["table_name"]} (
            {", ".join([f"{c["name"]} {col_types.get(c["name"], c["type"])}" for cols in t_conf["cols"] for c in cols])}
            {primary_key_q}
        )        
    """)
    db_cur.execute(f"DELETE FROM {t_conf["table_name"]}") # no point in storing old data
    db_cur.executemany(f"""
        INSERT INTO {t_conf["table_name"]} ({", ".join([c["name"] for cols in t_conf["cols"] for c in cols])}) 
        VALUES ({", ".join(["?"]*len([0 for cols in t_conf["cols"] for _ in cols]))})
    """, df.values.tolist())
    logging.info(f"TABLE '{t_conf["table_name"]}' updated")


def update_db():
    upd_con = sqlite3.connect(db_f)
    try:
        upd_cur = upd_con.cursor()
        for t_conf in table_conf:
            # TODO: check if I should make a cursor and commit once, or once per function call
            update_table(t_conf, upd_cur)
        upd_con.commit() # TODO: last updared should come from here
        logging.info("DB update finished")
    except BaseException as e:
        logging.info(f"DB update FAILED - {e}")
    finally:
        upd_con.close()


update_db()
