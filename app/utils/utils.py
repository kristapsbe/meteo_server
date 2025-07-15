char_map = {
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

# this is still mangled - just remove non-ASCII params

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
