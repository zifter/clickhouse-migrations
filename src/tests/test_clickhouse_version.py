import pytest

from tests.clickhouse_version import parse_version, skip_unless_server_at_least


@pytest.mark.parametrize(
    "text, expected",
    [
        ("25.7.4.11", (25, 7, 4, 11)),
        ("23.3", (23, 3)),
        (" 24.8.14.39 \n", (24, 8, 14, 39)),
        ("26.9.1.1-stable", (26, 9, 1, 1)),
    ],
)
def test_parse_version(text, expected):
    assert parse_version(text) == expected


def test_parse_version_rejects_garbage():
    with pytest.raises(ValueError, match="not a ClickHouse version: 'latest'"):
        parse_version("latest")


def test_parse_version_compares_numerically():
    assert parse_version("23.10") > parse_version("23.8")
    assert parse_version("23.3") < parse_version("23.3.1")


@pytest.mark.parametrize("server", ["23.3.1.1", "23.3", "25.7.4.11", "100.1"])
def test_no_skip_on_the_minimum_or_newer(server):
    skip_unless_server_at_least(server, "23.3", "KeeperMap")


def test_skips_on_an_older_server():
    with pytest.raises(pytest.skip.Exception) as exc_info:
        skip_unless_server_at_least("22.8.21.38", "23.3", "KeeperMap")
    assert str(exc_info.value) == (
        "needs ClickHouse 23.3 or newer (KeeperMap), the server is 22.8.21.38"
    )
