char_map = {
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
    'ž': 'z'
}


def simlpify_string(s):
    return ''.join([char_map.get(c, c) for c in s])


hourly_params = "','".join([
    'Laika apstākļu piktogramma',
    'Temperatūra (°C)',
    'Sajūtu temperatūra (°C)',
    'Vēja ātrums (m/s)',
    'Vēja virziens (°)',
    'Brāzmas (m/s)',
    'Nokrišņi (mm)',
    'UV indekss (0-10)',
    'Pērkona negaisa varbūtība (%)',
])

daily_params = "','".join([
    'Diennakts vidējā vēja vērtība (m/s)',
    'Diennakts maksimālā vēja brāzma (m/s)',
    'Diennakts maksimālā temperatūra (°C)',
    'Diennakts minimālā temperatūra (°C)',
    'Diennakts nokrišņu summa (mm)',
    'Diennakts nokrišņu varbūtība (%)',
    'Laika apstākļu piktogramma nakti',
    'Laika apstākļu piktogramma diena',
])


def get_params(cur, param_q):
    return cur.execute(f"""
        SELECT
            id, title_lv, title_en
        FROM
            forecast_cities_params
        WHERE
            title_lv in ('{param_q}')
    """).fetchall()
