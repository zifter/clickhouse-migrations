.PHONY: setup
setup:
	python3 -m pip install tox==4.8.0

.PHONY: setup-dev
setup-dev:
	pyenv install -s 3.7.12
	pyenv install -s 3.8.6
	pyenv install -s 3.9.9
	pyenv install -s 3.10.7
	pyenv install -s 3.11.0
	pyenv install -s 3.12-dev
	pyenv local 3.7.12 3.8.6 3.9.9 3.10.7 3.11.0 3.12-dev
	python3 -m pip install tox==4.8.0

.PHONY: test
test:
	tox

.PHONY: fix
fix:
	tox -e isort
	tox -e black

.PHONY: build
build:
	tox -e build_wheel

.PHONY: publish
publish:
	tox -e pypi_upload

.PHONY: open-coverage
open-coverage:
	xdg-open htmlcov/index.html || open htmlcov/index.html

.PHONY: docker-compose-up
docker-compose-up:
	cd dev && make configure up

.PHONY: docker-compose-down
docker-compose-down:
	cd dev && make down clean
