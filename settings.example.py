warning_mode = False
db_file = "meteo.db"
if warning_mode:
    db_file = "meteo_warning_test.db"

data_folder = "data/"
if warning_mode:
    data_folder = "data_warnings/"

editdist_extension = '/path/to/.sqlpkg/sqlite/spellfix/spellfix.dylib'
