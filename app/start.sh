if [ ! -f /data/meteo.db ]; then
    sh run_job.sh download
fi

crond
# I think this isn't working properly
# TODO: check if condition is correct
if [ "$DO_RUN_AURORA" = "true" ]; then \
    sh run_job.sh download_aurora & uv run main.py
else
    uv run main.py
fi
