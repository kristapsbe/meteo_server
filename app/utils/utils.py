import string


char_map = {
    # not discarding some special characters
    ' ': ' ',
    '(': '(',
    ')': ')',
    # lv
    'ā': 'a',
    'č': 'c',
    'ē': 'e',
    'ģ': 'g',
    'ī': 'i',
    'ķ': 'k',
    'ļ': 'l',
    'ņ': 'n',
    'š': 's',
    'ū': 'u',
    # ru
    'ž': 'z',
    'а': 'a',
    'б': 'b',
    'в': 'v',
    'г': 'g',
    'д': 'd',
    'ђ': 'd',
    'е': 'e',
    'ж': 'z',
    'з': 'z',
    'и': 'i',
    'ј': 'j',
    'к': 'k',
    'л': 'l',
    'љ': 'q',
    'м': 'm',
    'н': 'n',
    'њ': 'w',
    'о': 'o',
    'п': 'p',
    'р': 'r',
    'с': 's',
    'т': 't',
    'ћ': 'c',
    'у': 'u',
    'ф': 'f',
    'х': 'h',
    'ц': 'c',
    'џ': 'y',
    'ш': 's',
    # lt
    'ą': 'a',
   	'ę': 'e',
   	'ė': 'e',
    'į': 'i',
    'ų': 'u',
    # ee
    'õ': 'o',
    'ä': 'a',
    'ö': 'o',
    'ü': 'u',
    # by
    'э': 'e',
   	'ґ': 'g',
    'ё': 'e',
    'я': 'a',
    'і': 'i',
    'й': 'i',
    'ў': 'y',
    'ь': '',
    'ч': 'c',
    'ы': 'i',
    'ю': 'u',
    # pl
    'ć': 'c',
    'ł': 'l',
    'ń': 'n',
    'ó': 'o',
    'ś': 's',
    'ź': 'z',
    'ż': 'z',
}
for c in string.ascii_lowercase:
    char_map[c] = c


def simlpify_string(s: str) -> str:
    return ''.join([char_map.get(c, '') for c in s])


hourly_params = [
    1, # Laika apstākļu ikona
    2, # Temperatūra
    3, # Sajūtu temperatūra
    4, # Vēja ātrums
    5, # Vēja virziens
    6, # Brāzmas
    7, # Nokrišņi
    #8, # Atmosfēras spiediens
    #9, # Gaisa relatīvais mitrums
    10, # UV indeks
    11, # Pērkona varbūtība
]

daily_params = [
    #12, # Diennakts vidējais vēja virziens
    13, # Diennakts vidējā vēja vērtība
    14, # Diennakts maksimālā vēja brāzma
    15, # Diennakts maksimālā temperatūra
    16, # Diennakts minimālā temperatūra
    17, # Diennakts nokrišņu summa
    18, # Diennakts nokrišņu varbūtība
    19, # Laika apstākļu ikona nakti
    20, # Laika apstākļu ikona diena
]

lt_hourly_params = {
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

lt_day_icons = {
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

lt_night_icons = {
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

lt_icon_prio = [
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

lt_daily_params = {
    #12, # Diennakts vidējais vēja virziens
    'windSpeed': [(lambda d, n: sum(d+n)/len(d+n), 13)], # Diennakts vidējā vēja vērtība
    'windGust': [(lambda d, n: max(d+n), 14)], # Diennakts maksimālā vēja brāzma
    'airTemperature': [
        (lambda d, n: max(d+n), 15), # Diennakts maksimālā temperatūra
        (lambda d, n: min(d+n), 16), # Diennakts minimālā temperatūra
    ],
    'totalPrecipitation': [
        (lambda d, n: sum(d+n), 17), # Diennakts nokrišņu summa
        # (lambda a: sum(a), 18), # Diennakts nokrišņu varbūtība
    ],
    'conditionCode': [
        (lambda _, n: lt_night_icons[[e for e in lt_icon_prio if e in n][0]], 19), # Laika apstākļu ikona nakti
        (lambda d, _: lt_day_icons[[e for e in lt_icon_prio if e in d][0]], 20), # Laika apstākļu ikona diena
    ]
}
