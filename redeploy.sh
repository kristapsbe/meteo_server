cd ~/meteo_server
dnf update -y
git checkout main
git pull
docker compose build
docker compose up -d
