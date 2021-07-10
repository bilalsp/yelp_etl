"""
Main file to run the ETL jobs.
"""
from py4j.protocol import Py4JJavaError
from pydoc import locate

from src.app import SparkApp
from src import jobs

if __name__ == '__main__':

    app = SparkApp(config_file='configs/config.yaml')
    logger = app.log4jLogging.getLogger(__name__)

    for job_class_name in app.config['jobs'].keys():
        try:
            job_class = locate(job_class_name) or getattr(jobs, job_class_name)
            job = job_class(app)
            logger.info('Running {} ETL job.'.format(job.name))
            job.run()
        except (Exception, Py4JJavaError) as ex:
            ex = ex.java_exception if isinstance(ex, Py4JJavaError) else ex
            logger.warn(str(ex))
