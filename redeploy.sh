docker compose build
docker compose rm -s -f haproxy
docker compose up -d
docker compose rm -s -f app
docker compose up -d
