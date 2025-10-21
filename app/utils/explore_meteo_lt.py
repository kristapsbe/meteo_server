import json
import requests
import pandas as pd

from utils import simlpify_string


places = [
    e
    for e in json.loads(requests.get("https://api.meteo.lt/v1/places").content)
    if e["countryCode"] != "LV"
]

f_places = [
    [
        p["code"],  # id
        "LT",  # source
        p["name"],  # name
        simlpify_string(p["name"].strip().lower()),  # search_name
        p["coordinates"]["latitude"],  # lat
        p["coordinates"]["longitude"],  # lon
        "location_LT",  # type
        p["administrativeDivision"],  # county
        p["countryCode"],  # country
    ]
    for p in places
]

# print(pd.DataFrame(f_places).values.tolist())
# print(f_places)

# countries = [p['countryCode'] for p in places]

hourly_params = {
    # is the condition code the icon?
    "conditionCode": 1,  # Laika apstākļu ikona
    "airTemperature": 2,  # Temperatūra
    "feelsLikeTemperature": 3,  # Sajūtu temperatūra
    "windSpeed": 4,  # Vēja ātrums
    "windDirection": 5,  # Vēja virziens
    "windGust": 6,  # Brāzmas
    "totalPrecipitation": 7,  # Nokrišņi
    # 8, # Atmosfēras spiediens
    # 9, # Gaisa relatīvais mitrums
    # 'missing_1': 10, # UV indeks
    # 'missing_2': 11, # Pērkona varbūtība
}

daily_params = {
    # 12, # Diennakts vidējais vēja virziens
    "windSpeed": [
        (lambda d, n: sum(d + n) / len(d + n), 13)
    ],  # Diennakts vidējā vēja vērtība
    "windGust": [(lambda d, n: max(d + n), 14)],  # Diennakts maksimālā vēja brāzma
    "airTemperature": [
        (lambda d, n: max(d + n), 15),  # Diennakts maksimālā temperatūra
        (lambda d, n: min(d + n), 16),  # Diennakts minimālā temperatūra
    ],
    "totalPrecipitation": [
        (lambda d, n: sum(d + n), 17),  # Diennakts nokrišņu summa
        # (lambda a: sum(a), 18), # Diennakts nokrišņu varbūtība
    ],
    "conditionCode": [
        (
            lambda _, n: night_icons[[e for e in icon_prio if e in n][0]],
            19,
        ),  # Laika apstākļu ikona nakti
        (
            lambda d, _: day_icons[[e for e in icon_prio if e in d][0]],
            20,
        ),  # Laika apstākļu ikona diena
    ],
}

day_icons = {
    "clear": "1101",
    "partly-cloudy": "1102",
    "cloudy-with-sunny-intervals": "1103",
    "cloudy": "1104",
    "light-rain": "1503",
    "rain": "1505",
    "heavy-rain": "1504",
    "thunder": "1324",
    "isolated-thunderstorms": "1312",
    "thunderstorms": "1323",
    "heavy-rain-with-thunderstorms": "1310",
    "light-sleet": "1319",
    "sleet": "1318",
    "freezing-rain": "1317",
    "hail": "1317",
    "light-snow": "1602",
    "snow": "1601",
    "heavy-snow": "1604",
    "fog": "1401",
    "null": "0000",
}

night_icons = {
    "clear": "2101",
    "partly-cloudy": "2102",
    "cloudy-with-sunny-intervals": "2103",
    "cloudy": "2104",
    "light-rain": "2503",
    "rain": "2505",
    "heavy-rain": "2504",
    "thunder": "2324",
    "isolated-thunderstorms": "2312",
    "thunderstorms": "2323",
    "heavy-rain-with-thunderstorms": "2310",
    "light-sleet": "2319",
    "sleet": "2318",
    "freezing-rain": "2317",
    "hail": "2317",
    "light-snow": "2602",
    "snow": "2601",
    "heavy-snow": "2604",
    "fog": "2401",
    "null": "0000",
}

icon_prio = [
    "heavy-rain-with-thunderstorms",
    "thunderstorms",
    "isolated-thunderstorms",
    "thunder",
    "heavy-snow",
    "snow",
    "light-snow",
    "freezing-rain",
    "hail",
    "sleet",
    "light-sleet",
    "heavy-rain",
    "rain",
    "light-rain",
    "fog",
    "partly-cloudy",
    "cloudy",
    "cloudy-with-sunny-intervals",
    "clear",
]


for p in places:
    place_data = json.loads(
        requests.get(
            f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term"
        ).content
    )

    # print(place_data)

    h_dates = []
    all_forecasts = sorted(
        [e["forecastTimeUtc"] for e in place_data["forecastTimestamps"]]
    )
    for i in range(len(all_forecasts) - 1):
        if int(all_forecasts[i][11:13]) - int(all_forecasts[i + 1][11:13]) in {-1, 23}:
            print(
                all_forecasts[i],
                all_forecasts[i + 1],
                int(all_forecasts[i][11:13]) - int(all_forecasts[i + 1][11:13]),
            )
            h_dates.append(all_forecasts[i])
        else:
            break
    h_dates = set(h_dates)
    d_dates = set([e["forecastTimeUtc"][:10] for e in place_data["forecastTimestamps"]])

    params = [
        [
            p["code"],
            hourly_params[k],
            f["forecastTimeUtc"]
            .replace(" ", "")
            .replace("-", "")
            .replace(":", "")[:12],
            day_icons[v] if k == "conditionCode" else v,
        ]
        for f in place_data["forecastTimestamps"]
        for k, v in f.items()
        if f["forecastTimeUtc"] in h_dates and k in hourly_params
    ]
    # print(h_params)
    sorted_d_dates = sorted(list(d_dates))
    all_h_dates = list(
        set([e["forecastTimeUtc"] for e in place_data["forecastTimestamps"]])
    )
    # print(sorted_d_dates)
    for i in range(1, len(sorted_d_dates)):
        tmp_day = [
            e
            for e in place_data["forecastTimestamps"]
            if e["forecastTimeUtc"] >= f"{sorted_d_dates[i - 1][:10]} 09:00:00"
            and e["forecastTimeUtc"] < f"{sorted_d_dates[i - 1][:10]} 21:00:00"
        ]
        tmp_night = [
            e
            for e in place_data["forecastTimestamps"]
            if e["forecastTimeUtc"] >= f"{sorted_d_dates[i - 1][:10]} 21:00:00"
            and e["forecastTimeUtc"] < f"{sorted_d_dates[i][:10]} 09:00:00"
        ]
        for k in tmp_day[0].keys():
            if k in daily_params:
                for f in daily_params[k]:
                    # pass
                    params.append(
                        [
                            p["code"],
                            f[1],
                            f"{tmp_day[0]['forecastTimeUtc'].replace(' ', '').replace('-', '').replace(':', '')[:8]}0000",
                            f[0]([e[k] for e in tmp_day], [e[k] for e in tmp_night]),
                        ]
                    )

    print(params)
    # print(d_dates)
    # print(d_dates)
    # print([e['forecastTimeUtc'] for e in json.loads(requests.get(f"https://api.meteo.lt/v1/places/{p['code']}/forecasts/long-term").content)['forecastTimestamps']])
    break
