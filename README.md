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

```
python main.py
```

2*cores
```
uvicorn main:app --workers 2
```

```
localhost:8000/api/v1/forecast/cities
```

```
locust --host http://localhost:8000
```