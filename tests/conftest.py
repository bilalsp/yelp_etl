"""
Testsuite resource fixture to create the spark application.
"""
import pytest

from src.app import SparkApp


@pytest.fixture(scope='session', autouse=True)
def resource():
    app = SparkApp(config_file='configs/config_test.yaml')
    app.sparkContext.setLogLevel("WARN")
    print(app.id)
    yield app
    app.stop()
