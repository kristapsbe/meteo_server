import json
import time
import requests


resp = requests.post(
    "https://api.uptimerobot.com/v2/getMonitors",
    data={
        "api_key": "ur2776791-4c7590ba527a87c148e93db6",
        "logs": 1
    }
)

j = json.loads(resp.content)
#print(j["monitors"])
#print()
#print([e["friendly_name"] for e in j["monitors"]])

upt = [
    '/api/v1/forecast/cities (DOWN if city name is missing)',
    '/api/v1/forecast/cities (DOWN if daily forecast is an empty list)',
    '/api/v1/forecast/cities (DOWN if hourly forecast is an empty list)',
    '/api/v1/forecast/cities (DOWN if status is not 2xx or 3xx)',
    '/api/v1/forecast/cities/name (DOWN if city name is missing)',
    '/api/v1/forecast/cities/name (DOWN if daily forecast is an empty list)',
    '/api/v1/forecast/cities/name (DOWN if hourly forecast is an empty list)',
    '/api/v1/forecast/cities/name (DOWN if status is not 2xx or 3xx)',
    '/api/v1/meta (DOWN if status is not 2xx or 3xx)',
    '/api/v1/version (DOWN if status is not 2xx or 3xx)',
    '/privacy-policy (DOWN if page title is missing)',
    '/privacy-policy (DOWN if status is not 2xx or 3xx)'
]

meta = [
    '/api/v1/meta (DOWN if aurora forecast is out of date)',
    '/api/v1/meta (DOWN if forecast download fallback has failed)',
    '/api/v1/meta (DOWN if forecast download has failed)',
]

upt = [
    '/api/v1/forecast/cities (DOWN if city name is missing)'
]

print(int(time.time()))

for e in j["monitors"]:
    if e["friendly_name"] in upt:
        print(e["create_datetime"])
        print(e["logs"])
