import os
import json
import time
import pandas as pd
import logging
import uvicorn
import requests
import threading

from fastapi import FastAPI


app = FastAPI()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

base_url = "https://data.gov.lv/dati/api/3/"
data_f = "data/"
raw_f = f"{data_f}raw/"
parsed_f = f"{data_f}parsed/"


def refresh(url, fpath, reload, verify_download):
    valid_new = False
    if not os.path.exists(fpath) or time.time()-os.path.getmtime(fpath) > reload:
        logging.info(f"Downloading {fpath}")
        r = requests.get(url)
        if r.status_code == 200 and verify_download(r.content):
            with open(fpath, "wb") as f: # this can be eiher a json or csv
                f.write(r.content)
            valid_new = True
    else:
        logging.info(f"A recent version of {fpath} exists - not downloading ({int(time.time()-os.path.getmtime(fpath))})")
    return valid_new


def verif_json(s):
    try:
        return json.loads(s)['success'] == True
    except:
        return False


verif_funcs = {
    "json": verif_json,
    "csv": lambda s: True
}


def download_resources(ds_name, reload):
    ds_path = f"{raw_f}{ds_name}.json"
    valid_new = refresh(f"{base_url}action/package_show?id={ds_name}", ds_path, reload, verif_funcs['json'])
    ds_data = {}
    if valid_new: # don't want to download new data csv's unless I get a new datasource json first
        ds_data = json.loads(open(ds_path, "r").read())
        for r in ds_data['result']['resources']:
            refresh(r['url'], f"{raw_f}{ds_name}/{r['url'].split('/')[-1]}", reload, verif_funcs['csv'])


target_ds = {
    # TODO: using warnings would be cool - it has both the warning texts and geo polygon that it applies to
    "hidrometeorologiskie-bridinajumi": 900,
    # really nice hourly forecast data
    "meteorologiskas-prognozes-apdzivotam-vietam": 900
}

for ds in target_ds:
    os.makedirs(f"{raw_f}{ds}/", exist_ok=True)
os.makedirs(f"{parsed_f}cities/", exist_ok=True)


def get_city_data(city_id):
    # TODO: fix, just messing around atm
    # the pram csv is slightly broken - there's an extra comma
    params = {} # TODO: I think I should hard-code both filenames and params
    with open(f"{raw_f}meteorologiskas-prognozes-apdzivotam-vietam/forcity_param.csv", "r") as f: # hard code param white-list?
        for l in f.readlines()[1:]:
            parts = l.split(",")
            params[int(parts[0])] = parts[1]
    # this is hourly data - the _day.csv may be a lot more useful for showing 
    # results at a glance
    df = pd.read_csv(f"{raw_f}meteorologiskas-prognozes-apdzivotam-vietam/forecast_cities.csv")
    df = df[df['CITY_ID'] == city_id] # Rīga
    dates = sorted(df['DATUMS'].unique()) # YYYY-MM-DD HH:mm:SS - sortable as strings

    ds_json = json.loads(open(f"{raw_f}meteorologiskas-prognozes-apdzivotam-vietam.json", "r").read())
    output = {
        'params': [],
        'dates': {d: [] for d in dates},
        'last_modified': [e for e in ds_json['result']['resources'] if "forecast_cities.csv" in e["url"]][0]['last_modified']
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


def regen_city_forecasts():
    with open(f"{parsed_f}cities/Riga.json", "w") as f:
        f.write(json.dumps(get_city_data('P28')))
    
    tmp_df = pd.read_csv(f"{raw_f}hidrometeorologiskie-bridinajumi/bridinajumu_metadata.csv")
    logging.info("===========================================================================")
    logging.info(tmp_df[['PARADIBA', 'INTENSITY_LV', 'TIME_FROM', 'TIME_TILL']])
    logging.info("===========================================================================")
        

def run_downloads(datasets):
    try:
        logging.info(f"Triggering refresh")
        for ds, reload in datasets.items():
            download_resources(ds, reload)
        regen_city_forecasts()
    except:
        logging.info("Refresh failed")
    finally:
        # TODO: this is kind-of dumb - do I actually want to trigger multiple 
        # timers and keep re-checking files? it does mean that I'll download
        # stuff reasonably quickly if stuff fails for some reason
        timer = threading.Timer(30.0, run_downloads, [datasets])
        timer.start()

run_downloads(target_ds)


@app.get("/api/v1/forecast/cities") # TODO: take city center coords, and get data within n km (?)
async def download_dataset():
    return [json.loads(open(f"{parsed_f}cities/{d}.json", "r").read()) for d in ["Riga"]]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
