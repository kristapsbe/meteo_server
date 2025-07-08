import json
import requests


places = json.loads(requests.get("https://api.meteo.lt/v1/places").content)
print(places)
countries = [p['countryCode'] for p in places]
print({c: countries.count(c) for c in set(countries)}) # it's not just LT

for p in places:
    print(json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content))
    break
