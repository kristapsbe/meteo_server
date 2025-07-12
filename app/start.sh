if [ ! -f /data/meteo.db ]; then
    sh run_job.sh download
elif [ "$DO_RUN_AURORA" = "true" ]; then \
    sh run_job.sh download_aurora
fi

crond
uv run main.py
