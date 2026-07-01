import os


def pytest_collection_modifyitems(items):
    """Auto-mark everything under tests/integration as requiring ClickHouse."""
    integration_marker = f"{os.sep}integration{os.sep}"
    for item in items:
        if integration_marker in str(item.fspath):
            item.add_marker("integration")
