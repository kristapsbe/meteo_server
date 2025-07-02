run_attached:
	# TODO: looks like $(uname -p) isnt working properly - fix
	docker compose build --no-cache --build-arg HOST_ARCHITECTURE="$(uname -p)" && docker compose up
