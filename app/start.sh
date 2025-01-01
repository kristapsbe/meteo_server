if [ ! -f /data/meteo.db ]; then
    sh run_job.sh download
fi

uv run main.py
