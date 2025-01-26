cd ~/meteo_server
dnf update -y # unsure if this is a good idea - may cause a lot of second long downtimes
git checkout main

changed=0
git status -uno | grep -q 'Your branch is behind' && changed=1
if [ $changed = 1 ]; then
    git pull
    git rev-parse HEAD > app/version.txt
    docker compose build
    docker compose up -d
fi
