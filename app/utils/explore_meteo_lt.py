import json
import requests

from utils import simlpify_string


places = [e for e in json.loads(requests.get("https://api.meteo.lt/v1/places").content) if e['countryCode'] != "LV"]

f_places = [{
    'id': p['code'],
    'source': 'LT',
    'name': p['name'],
    'search_name': simlpify_string(p['name'].strip().lower()),
    'lat': p['coordinates']['latitude'],
    'lon': p['coordinates']['longitude'],
    'type': 'location_LT',
    'county': p['administrativeDivision'],
    'country': p['countryCode'],
} for p in places]
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
