import json
import requests
import pandas as pd

from utils import simlpify_string


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

print(pd.DataFrame(f_places).values.tolist())
# print(f_places)

countries = [p['countryCode'] for p in places]

# hourly_params = [
#     1, # Laika apstākļu ikona
#     2, # Temperatūra
#     3, # Sajūtu temperatūra
#     4, # Vēja ātrums
#     5, # Vēja virziens
#     6, # Brāzmas
#     7, # Nokrišņi
#     #8, # Atmosfēras spiediens
#     #9, # Gaisa relatīvais mitrums
#     10, # UV indeks
#     11, # Pērkona varbūtība
# ]

# daily_params = [
#     #12, # Diennakts vidējais vēja virziens
#     13, # Diennakts vidējā vēja vērtība
#     14, # Diennakts maksimālā vēja brāzma
#     15, # Diennakts maksimālā temperatūra
#     16, # Diennakts minimālā temperatūra
#     17, # Diennakts nokrišņu summa
#     18, # Diennakts nokrišņu varbūtība
#     19, # Laika apstākļu ikona nakti
#     20, # Laika apstākļu ikona diena
# ]

for p in places:
    print([e['forecastTimeUtc'] for e in json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)['forecastTimestamps']])
    break
