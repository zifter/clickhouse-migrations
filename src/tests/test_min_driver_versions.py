import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
DRIVERS = ("clickhouse-driver", "clickhouse-connect")


def test_mindeps_env_pins_the_declared_lower_bounds():
    # tox -e py39-mindeps runs the suite against the lowest driver versions
    # pyproject.toml accepts; the two lists must not drift apart.
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    bounds = re.findall(r'"(clickhouse-(?:driver|connect))>=([^",]+)"', pyproject)
    tox_ini = (ROOT / "tox.ini").read_text(encoding="utf-8")
    env = tox_ini.split("[testenv:py39-mindeps]", 1)[1].split("\n[", 1)[0]
    pins = dict(re.findall(r"^\s+(clickhouse-(?:driver|connect))==(\S+)$", env, re.M))

    assert sorted(pins) == sorted(DRIVERS)
    assert {name for name, _ in bounds} == set(DRIVERS)
    for name, bound in bounds:
        assert pins[name] == bound, name
