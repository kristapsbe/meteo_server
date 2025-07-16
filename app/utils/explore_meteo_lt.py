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

# print(pd.DataFrame(f_places).values.tolist())
# print(f_places)

# countries = [p['countryCode'] for p in places]

hourly_params = {
    # is the condition code the icon?
    'conditionCode': 1, # Laika apstākļu ikona
    'airTemperature': 2, # Temperatūra
    'feelsLikeTemperature': 3, # Sajūtu temperatūra
    'windSpeed': 4, # Vēja ātrums
    'windDirection': 5, # Vēja virziens
    'windGust': 6, # Brāzmas
    'totalPrecipitation': 7, # Nokrišņi
    #8, # Atmosfēras spiediens
    #9, # Gaisa relatīvais mitrums
    # 'missing_1': 10, # UV indeks
    # 'missing_2': 11, # Pērkona varbūtība
}

daily_params = {
    #12, # Diennakts vidējais vēja virziens
    'windSpeed': [(lambda a: sum(a)/len(a), 13)], # Diennakts vidējā vēja vērtība
    'windGust': [(lambda a: max(a), 14)], # Diennakts maksimālā vēja brāzma
    'airTemperature': [
        (lambda a: max(a), 15), # Diennakts maksimālā temperatūra
        (lambda a: min(a), 16), # Diennakts minimālā temperatūra
    ],
    'totalPrecipitation': [
        (lambda a: sum(a), 17), # Diennakts nokrišņu summa
        (lambda a: sum(a), 18), # Diennakts nokrišņu varbūtība
    ],
    'conditionCode': [
        (lambda a: a, 19), # Laika apstākļu ikona nakti
        (lambda a: a, 20), # Laika apstākļu ikona diena
    ]
}

day_icons = {
    'clear': '1101',
    'partly-cloudy': '1102',
    'cloudy-with-sunny-intervals': '1103',
    'cloudy': '1104',
    'light-rain': '1503',
    'rain': '1505',
    'heavy-rain': '1504',
    'thunder': '1324',
    'isolated-thunderstorms': '1312',
    'thunderstorms': '1323',
    'heavy-rain-with-thunderstorms': '1310',
    'light-sleet': '1319',
    'sleet': '1318',
    'freezing-rain': '1317',
    'hail': '1317',
    'light-snow': '1602',
    'snow': '1601',
    'heavy-snow': '1604',
    'fog': '1401',
    'null': '0000',
}

night_icons = {
    'clear': '2101',
    'partly-cloudy': '2102',
    'cloudy-with-sunny-intervals': '2103',
    'cloudy': '2104',
    'light-rain': '2503',
    'rain': '2505',
    'heavy-rain': '2504',
    'thunder': '2324',
    'isolated-thunderstorms': '2312',
    'thunderstorms': '2323',
    'heavy-rain-with-thunderstorms': '2310',
    'light-sleet': '2319',
    'sleet': '2318',
    'freezing-rain': '2317',
    'hail': '2317',
    'light-snow': '2602',
    'snow': '2601',
    'heavy-snow': '2604',
    'fog': '2401',
    'null': '0000',
}

icon_prio = [
    'heavy-rain-with-thunderstorms',
    'thunderstorms',
    'isolated-thunderstorms',
    'thunder',
    'heavy-snow',
    'snow',
    'light-snow',
    'freezing-rain',
    'hail',
    'sleet',
    'light-sleet',
    'heavy-rain',
    'rain',
    'light-rain',
    'fog',
    'partly-cloudy',
    'cloudy',
    'cloudy-with-sunny-intervals',
    'clear',
]


for p in places:
    place_data = json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)

    # print(place_data)

    h_dates = []
    for i in range(len(place_data['forecastTimestamps'])-1):
        if int(place_data['forecastTimestamps'][i]['forecastTimeUtc'][11:13])-int(place_data['forecastTimestamps'][i+1]['forecastTimeUtc'][11:13]) in {-1, 23}:
            h_dates.append(place_data['forecastTimestamps'][i]['forecastTimeUtc'])
        else:
            break
    h_dates = set(h_dates)
    d_dates = set([e['forecastTimeUtc'][:10] for e in place_data['forecastTimestamps']])

    h_params = [[p['code'], hourly_params[k], f['forecastTimeUtc'].replace(" ", "").replace("-", "").replace(":", "")[:12], day_icons[v] if k == 'conditionCode' else v] for f in place_data['forecastTimestamps'] for k,v in f.items() if f['forecastTimeUtc'] if h_dates and k in hourly_params]

    print(h_params)
    # print(h_dates)
    # print(d_dates)
    # print([e['forecastTimeUtc'] for e in json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)['forecastTimestamps']])
    break
