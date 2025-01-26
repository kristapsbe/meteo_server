cd ~/meteo_server
dnf update -y # unsure if this is a good idea - may cause a lot of second long downtimes
git checkout main
git pull
docker compose build
docker compose up -d
