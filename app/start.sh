echo $UPTIMEROBOT

if [ ! -f /data/meteo.db ]; then
    sh run_job.sh download
fi

crond
uv run main.py
