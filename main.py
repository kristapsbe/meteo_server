import os
import json
import time
import pandas as pd
import logging
import requests
import threading

from fastapi import FastAPI


app = FastAPI()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"


def refresh(url, fpath, reload):
    # TODO: I should probably check if I actually need to make the dirs before I do
    os.makedirs("/".join(fpath.split("/")[:-1]), exist_ok=True)

    if not os.path.exists(fpath) or time.time()-os.path.getmtime(fpath) > reload:
        logging.info(f"{fpath} - downloading")
        r = requests.get(url)
        if r.status_code == 200:
            # TODO: check if we want to overwrite the file before writing (i.e. if the file doesn't look damaged)
            with open(fpath, "wb") as f: # this can be eiher a json or csv
                f.write(r.content)
    else:
        logging.info(f"{fpath} is too new - not downloading ({time.time()-os.path.getmtime(fpath)})")


def refresh_and_get_json(url, fpath, reload):
    refresh(url, fpath, reload)
    
    data = {}
    try:
        if os.path.exists(fpath):
            data = json.loads(open(fpath, "r").read())
    except:
        logging.info(f"{fpath} went bad - DELETE")
        os.remove(fpath) # couldn't parse the json - delete it so that we re-download next time
    return data


def download_resources(ds_name, reload):
    pl_data = refresh_and_get_json(f"{base_url}action/package_list", "data/package_list.json", reload)

    if pl_data['success'] and ds_name in pl_data['result']:
        ds_data = refresh_and_get_json(f"{base_url}action/package_show?id={ds_name}", f"data/{ds_name}.json", reload)
        if ds_data['success']:
            for r in ds_data['result']['resources']:
                refresh(r['url'], f"data/{r['url'].split('/')[-1]}", reload)


def run_downloads():
    try:
        for dr in [
            #"aktualie-celu-meteorologisko-staciju-dati", # some sort of mapserver page
            # TODO: using warnings would be cool - it has both the warning texts and geo polygon that it applies to
            "hidrometeorologiskie-bridinajumi",
            #"hidrometeorologiskie-noverojumi", # water level and temp stuff
            # TODO: skipping this for now - do I want a map at a later point?
            # "telpiskas-meteorologiskas-prognozes", # temp, humidity and pressure - no cloud coverage :/
            #"telpiskie-hidrometeorologiskie-noverojumi", # heavily delayed storm data
            "meteorologiskas-prognozes-apdzivotam-vietam" # really nice hourly forecast data
        ]:
            logging.info(f"refreshing {dr}")
            download_resources(dr, 900)
    except:
        logging.info("failed")
    finally:
        # TODO: this is kind-of dumb - do I actually want to trigger multiple 
        # timers and keep re-checking files? it does mean that I'll download
        # stuff reasonably quickly if stuff fails for some reason
        timer = threading.Timer(30.0, run_downloads)
        timer.start()

run_downloads()


def get_city_data():
    # TODO: fix, just messing around atm
    # the pram csv is slightly broken - there's an extra comma
    params = {} # TODO: I think I should hard-code both filenames and params
    with open("data/forcity_param.csv", "r") as f: # hard code param white-list?
        for l in f.readlines()[1:]:
            parts = l.split(",")
            params[int(parts[0])] = parts[1]
    # this is hourly data - the _day.csv may be a lot more useful for showing 
    # results at a glance
    df = pd.read_csv("data/forecast_cities.csv")
    df = df[df['CITY_ID'] == 'P28'] # Rīga
    dates = sorted(df['DATUMS'].unique()) # YYYY-MM-DD HH:mm:SS - sortable as strings
    output = {
        'params': [],
        'dates': {d: [] for d in dates}
    }

    for p in [int(v) for v in df['PARA_ID'].unique()]:
        if p in params:
            output['params'].append(params[p])

            tmp_df = df[df['PARA_ID'] == p]
            data = json.loads(tmp_df.to_json())
            tmp_data = {data['DATUMS'][k]: data['VERTIBA'][k] for k in data['CITY_ID'].keys()}

            for d in dates:
                output['dates'][d].append(tmp_data.get(d, None))

    return output


@app.get("/api/v1/forecast/cities")
async def download_dataset():
    tmp_df = pd.read_csv("data/bridinajumu_metadata.csv")
    logging.info("===========================================================================")
    logging.info(tmp_df[['PARADIBA', 'INTENSITY_LV', 'TIME_FROM', 'TIME_TILL']])
    logging.info("===========================================================================")
    return get_city_data()
