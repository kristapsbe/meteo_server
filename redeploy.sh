cd ~/meteo_server
dnf update -y # unsure if this is a good idea - may cause a lot of second long downtimes
git checkout main
git pull
git rev-parse HEAD > app/version.txt
docker compose build
docker compose up -d
