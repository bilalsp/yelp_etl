"""
The :mod:`src.logging` module includes logging class that wraps the log4j object
instantiated by active SparkContext and enable log4j logging for Spark App.
"""
from src.logging._logging import Log4j
