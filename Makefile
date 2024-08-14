run:
	git checkout main
	git pull
	git rev-parse HEAD > git.version
	python main.py
