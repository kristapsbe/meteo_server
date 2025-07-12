cd ~/meteo_server

[ -f "app/app.env" ] || cp app/app.example.env app/app.env

git checkout main

changed=0
git fetch && git status -uno | grep -q 'Your branch is behind' && changed=1
if [ $changed = 1 ]; then
    git pull
    git rev-parse HEAD > app/version.txt
    docker compose build --no-cache --build-arg HOST_ARCHITECTURE="$(uname -p)" --build-arg DO_RUN_AURORA=true
    docker compose rm -svf haproxy # making sure that the HAProxy container restarts so that new certs are picked up
    docker compose up -d

    docker system prune -a -f --volumes

    bash utils/install.sh
else
    docker compose up -d # making sure that stuff's running even if there's no update to pull
fi
