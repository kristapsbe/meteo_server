uv run certbot certonly -n -q --standalone --force-renew -d meteo.kristapsbe.lv
cat /etc/letsencrypt/live/meteo.kristapsbe.lv/fullchain.pem /etc/letsencrypt/live/meteo.kristapsbe.lv/privkey.pem > /certs/haproxy.pem
