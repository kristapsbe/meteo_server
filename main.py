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

    df = pd.read_csv("data/forecast_cities.csv")
    df = df[df['CITY_ID'] == 'P28']
    df = df[df['PARA_ID'] == 2]
    data = json.loads(df.to_json())
    return [{data['DATUMS'][k]: data['VERTIBA'][k]} for k in data['CITY_ID'].keys()]


@app.get("/{dataset_name}")
async def download_dataset(dataset_name):
    return download_resources(dataset_name)
