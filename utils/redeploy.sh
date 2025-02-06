cd ~/meteo_server
git checkout main

changed=0
git fetch && git status -uno | grep -q 'Your branch is behind' && changed=1
if [ $changed = 1 ]; then
    git pull
    git rev-parse HEAD > app/version.txt
    docker compose build
    docker compose rm -svf haproxy # making sure that the HAProxy container restarts so that new certs are picked up
    docker compose up -d

    docker system prune -a -f --volumes

    bash utils/install.sh
fi
