import json
import requests

from html import escape
from settings import api_key


#auth header
#f"Authorization:{api_key}"

base_url = "https://data.gov.lv/dati/api/3/"

sql = """
    SELECT * FROM "7af98218-6266-4459-a79d-a7dfe29277e0" ORDER BY ABS(ABS("Lat" - 50) + ABS("Lon" - 25))
"""


with open("tmp.json", "w") as f:
    f.write(
        json.dumps(
            json.loads(
                requests.get(
                    f"https://data.gov.lv/dati/api/3/action/datastore_search_sql?sql={sql.strip()}"
                ).content
            )
        )
    )

#http://demo.ckan.org/api/3/action/package_list
#http://demo.ckan.org/api/3/action/group_list
#http://demo.ckan.org/api/3/action/tag_list