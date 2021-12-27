.PHONY: setup
setup:
	python3 -m pip install tox

.PHONY: setup-dev
setup-dev:
	pyenv local 3.10.0 3.9.9 3.8.6 3.7.12 3.6.15 || true
	python3 -m pip install tox-pyenv

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
	tox -e upload

.PHONY: open-coverage
open-coverage:
	xdg-open htmlcov/index.html || open htmlcov/index.html

.PHONY: docker-compose-up
docker-compose-up:
	cd dev && make configure up

.PHONY: docker-compose-down
docker-compose-down:
	cd dev && make down clean
