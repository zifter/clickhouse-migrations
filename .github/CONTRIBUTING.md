# Contributing

Thanks for taking the time to contribute! This project is a small, actively
maintained library for running ClickHouse migrations, and contributions of all
sizes are welcome — bug reports, docs, and code.

## Ways to contribute

- **Report a bug** — open an issue with the Bug report template.
- **Request a feature** — open an issue with the Feature request template.
- **Send a pull request** — see the workflow below.

For anything large, it's best to open an issue first so we can agree on the
approach before you invest time.

## Development setup

Requirements: Python 3.9+, Docker (for the integration tests), and `make`.

```bash
# Install the tooling (tox)
make setup

# Install the package with test dependencies into your own venv
python -m pip install -e ".[testing]"
```

## Running the tests

The test suite is split into **unit** tests (no database needed) and
**integration** tests (require a running ClickHouse cluster).

```bash
# Unit tests only — fast, no ClickHouse required
pytest src -m "not integration"

# Full suite (unit + integration): start ClickHouse first
make docker-compose-up-deamon
make test            # runs tox: linters + tests + coverage
make docker-compose-down
```

Integration tests live under `src/tests/integration/` and are auto-marked with
the `integration` marker.

## Linting and formatting

```bash
make fix     # auto-format with isort + black
make test    # also runs flake8 and pylint via tox
```

CI enforces `isort`, `black`, `flake8`, `pylint` (10.00/10), and coverage
thresholds (99% for the package, 100% for tests), so please run the checks
locally before pushing.

## Migration file convention

Migration files are named `{VERSION}_{name}.sql`, e.g. `001_init.sql`. The
version is the integer prefix before the first `_` and must be unique within a
migrations directory.

## Pull request workflow

1. Fork the repo and create a topic branch from `main`.
2. Make your change, with tests. New behavior should be covered.
3. Run `make fix` and make sure the checks pass.
4. Add a note to `CHANGELOG.md` if the change is user-facing.
5. Open the PR against `main` with a clear description of the change and why.

Versioning and releases are handled by the maintainer via GitHub Releases.
