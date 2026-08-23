"""Pytest configuration scoped to repository tests."""


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers",
        "lab_integration: Ubuntu/Linux integration tests that require the maintained lab stack",
    )
