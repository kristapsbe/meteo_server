import json
import urllib
import requests

from html import escape
from settings import api_key

#auth header
#f"Authorization:{api_key}"

base_url = "https://data.gov.lv/dati/api/3/"

# table name is resource id
# e.g.
# https://data.gov.lv/dati/dataset/40d80be5-0c09-47c4-80f3-fad4bec19f33/resource/17460efb-ae99-4d1d-8144-1068f184b05f/download/meteo_operativie_dati.csv
# --> 17460efb-ae99-4d1d-8144-1068f184b05f <--

tables = {
    # Hidrometeoroloģiskie novērojumi
    "hidromet_obs": {
        "": ""
    },
    # Meteoroloģiskās prognozes apdzīvotām vietām 
    #
    # CITY_ID - unikāls pilsētas identifikācijas numurs;
    # NOSAUKUMS - apdzīvotās vietas nosaukums, norādīts latviešu valodā;
    # LAT - ģeogrāfiskais platums, grādos;
    # LON - ģeogrāfiskais garums, grādos.
    "cities": "5b196104-36dd-4a81-822c-f218f92c16dc",
    # PARAM_ID - unikāls meteoroloģiskā parametra identifikācijas numurs;
    # NOSAUKUMS - parametra nosaukums latviešu valodā;
    # DESCRIPTION - parametra nosaukums angļu valodā un mērvienības.
    "params": "ba9b0984-2385-4271-9a0d-5deeec8c73a3",
    # CITY_ID -, unikālais pilsētas id (norāda uz konkrētu ierakstu “Pilsētu saraksts” tabulā);
    # PARAM_ID - unikālais parametra id (norāda uz konkrētu ierakstu “Parametru saraksts” tabulā);
    # DATUMS - aktuāls prognozes laiks;
    # VERTIBA - prognozētā parametra vērtība.
    "forecast_hourly": "27ed5547-22fb-4e30-a25f-b69c5eb0224c",
    "forecast_daily": "c20bd3f3-1017-4d29-bea7-110d00dacbb7",


}

# vietu saraksts
# 5b196104-36dd-4a81-822c-f218f92c16dc
# acde4e45-ceb2-4054-8f48-5df108110f6e
sql = """
    SELECT * FROM "ba9b0984-2385-4271-9a0d-5deeec8c73a3" 
"""

params = urllib.parse.urlencode({
    "sql": sql
})

with open("cities.json", "w") as f:
    f.write(
        json.dumps(
            json.loads(
                requests.get(
                    f"{base_url}action/datastore_search_sql?{params}"
                ).content
            )
        )
    )

sql = """
    SELECT * FROM "27ed5547-22fb-4e30-a25f-b69c5eb0224c" WHERE "CITY_ID" = 'P65'
"""

params = urllib.parse.urlencode({
    "sql": sql
})

with open("tmp.json", "w") as f:
    f.write(
        json.dumps(
            json.loads(
                requests.get(
                    f"{base_url}action/datastore_search_sql?{params}"
                ).content
            )
        )
    )

#http://demo.ckan.org/api/3/action/package_list
#http://demo.ckan.org/api/3/action/group_list
#http://demo.ckan.org/api/3/action/tag_list