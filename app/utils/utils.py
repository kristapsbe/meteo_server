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


def simlpify_string(s: str) -> str:
    return ''.join([char_map.get(c, c) for c in s])


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
