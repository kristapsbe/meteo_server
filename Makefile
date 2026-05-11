run_attached:
	docker compose build \
	    --no-cache --build-arg HOST_ARCHITECTURE="$$(uname -p)" \
		--build-arg DO_RUN_AURORA=true && docker compose up

terminal_app:
	docker exec -it meteo_server-app-1 sh

terminal_hap:
	docker exec -it meteo_server-haproxy-1 sh
