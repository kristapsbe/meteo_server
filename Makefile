run_attached:
	docker compose build \
	    --no-cache --build-arg HOST_ARCHITECTURE="$$(uname -p)" \
		--build-arg DO_RUN_AURORA=true && docker compose up

upgrade_deps:
	docker exec meteo_server-app-1 uv lock --upgrade
	docker cp meteo_server-app-1:/app/uv.lock app/uv.lock
	docker exec meteo_server-haproxy-1 uv lock --upgrade
	docker cp meteo_server-haproxy-1:/haproxy/uv.lock haproxy/uv.lock

terminal_app:
	docker exec -it meteo_server-app-1 sh

terminal_hap:
	docker exec -it meteo_server-haproxy-1 sh
