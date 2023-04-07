import pytest


@pytest.fixture(scope="session")
def sample_post_response_data():
    """mock data response from flink sql-gateway"""
    yield {}
    yield {}
