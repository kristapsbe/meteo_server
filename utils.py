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