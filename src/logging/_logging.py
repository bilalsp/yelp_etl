"""
Create a Logger for spark application.
"""


class Log4j:
    
    def __init__(self, spark, *args, **kwargs):
        """Wrapper class for Log4j JVM object.

        Parameters
        ----------
        spark : session object of spark application
        """
        super(Log4j, self).__init__(*args, **kwargs)
        self.log4jLogging = spark._jvm.org.apache.log4j.LogManager
        
    def error(self, message: str) -> None:
        """Log an error"""
        self.logger.error(message)

    def warn(self, message) -> None:
        """Log a warning"""
        self.logger.warn(message)

    def info(self, message) -> None:
        """Log information"""
        self.logger.info(message)
