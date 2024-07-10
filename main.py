import os
import json
import time
import pandas as pd
import requests

from fastapi import FastAPI


app = FastAPI()

base_url = "https://data.gov.lv/dati/api/3/"

pl_file = "data/package_list.json"


def refresh(url, fpath, reload=3600):
    # TODO: I should probably check if I actually need to make the dirs before I do
    os.makedirs("/".join(fpath.split("/")[:-1]), exist_ok=True)

    if not os.path.exists(fpath) or os.path.getmtime(fpath)-time.time() > reload:
        r = requests.get(url)
        if r.status_code == 200:
            with open(fpath, "wb") as f:
                f.write(r.content)


def refresh_and_get_json(url, fpath, reload=3600):
    refresh(url, fpath, reload)
    
    data = {}
    if os.path.exists(fpath):
        data = json.loads(open(fpath, "r").read())

    return data


def download_resources(ds_name):
    pl_data = refresh_and_get_json(f"{base_url}action/package_list", pl_file)

    if pl_data['success'] and ds_name in pl_data['result']:
        ds_data = refresh_and_get_json(f"{base_url}action/package_show?id={ds_name}", f"data/{ds_name}.json")
        if ds_data['success']:
            for r in ds_data['result']['resources']:
                refresh(r['url'], f"data/{r['url'].split('/')[-1]}")


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
        download_resources(dr)
    tmp_df = pd.read_csv("data/bridinajumu_metadata.csv")
    print("===========================================================================")
    print(tmp_df[['PARADIBA', 'INTENSITY_LV', 'TIME_FROM', 'TIME_TILL']])
    print("===========================================================================")
    return get_city_data()
