https://pipx.pypa.io/stable/installation/

set up virtualenv
```bash
pipx install virtualenv
```

create virtual env
```bash
python3 -m venv .venv
```

activate virtual env
```bash
source .venv/bin/activate
```

check that you're using the right env
```bash
which python
```

install deps
```bash
python -m pip install -r requirements.txt
```

when done
```bash
deactivate
```

# endpoints

```bash
# note - it doesn't look like this requires auth of any kind
https://data.gov.lv/dati/api/3/action/package_list # first gets a list of valid packages (probably irrelevant step)
https://data.gov.lv/dati/api/3/action/package_show?id=<dataset_name> # get the current list of valid resources
root->result->resources->$n->url # get url to current resources .csv file and download it
# we don't want to ask them for data via the SQL api - it's very (and I do mean very) slow
#
# at this point we've got a couple of tables that we can work with (starting with the 9 day forecast for inhabitet locations)
# looks like the main csv fails weighs only 70megs, and I know I'm mostly going to read, writing to the db once an hour or so (maybe 30 mins?) -> sqlite
# or, instead of using a db -> use pandas, and keep the table in-memory, and just filter when needed
```

use python, fastapi, gunicorn and pandas (?), and a cron job to restart the server if it decides to die?
https://stackoverflow.com/questions/2366693/run-cron-job-only-if-it-isnt-already-running