cd ~/meteo_server
git checkout main

changed=0
git fetch && git status -uno | grep -q 'Your branch is behind' && changed=1
if [ $changed = 1 ]; then
    git pull
    git rev-parse HEAD > app/version.txt
    docker compose build
    docker compose up -d

    bash utils/install.sh
fi
