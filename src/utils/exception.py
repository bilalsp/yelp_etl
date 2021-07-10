"""
Exception Classes for Spark Application.
"""


class SparkException(Exception):
    def __init__(self, message):
        super().__init__(message)


class AppNotCreatedException(SparkException):
    """ Failed to initialze Spark session."""
    def __init__(self, message, app_name):
        self.error_msg = 'Session of spark app {} not initialized\n{}'\
            .format(app_name, message)
        super(AppNotCreatedException, self).__init__(self.error_msg)


class BigQueryException(SparkException):
    """ """
    def __init__(self, message, job):
        self.error_msg = 'Spark job {}. Could not read or write to BigQuery\n{}'\
            .format(job, message)
        super(BigQueryException, self).__init__(self.error_msg)


class InvalidActionException(SparkException):
    def __init__(self, message):
        self.error_msg = '{} Valid action required to run the job.'.format(message)
        super(InvalidActionException, self).__init__(self.error_msg)


class FileNotFoundException(SparkException):
    """ Failed to fetch a file from secondary storage."""
    def __init__(self, message, file_path):
        self.error_msg = "File doesn't not exist or couldn't be fetched from {}\n{}"\
                         .format(file_path, message)
        super(FileNotFoundException, self).__init__(self.error_msg)
