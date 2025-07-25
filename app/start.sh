if [ ! -f /data/meteo.db ]; then
    sh run_job.sh download
fi

crond
if [ "$DO_RUN_AURORA" = "true" ]; then \
    sh run_job.sh download_every_20_minutes & uv run main.py
else
    uv run main.py
fi
