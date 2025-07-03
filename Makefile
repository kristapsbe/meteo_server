run_attached:
	docker compose build --no-cache --build-arg HOST_ARCHITECTURE="$$(uname -p)" && docker compose up

run_app_bash:
	docker exec -it meteo_server-app-1 sh
