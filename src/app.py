"""
Create Spark Application.
"""
import yaml
from os import environ

from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

from src.logging import Log4j
from src.utils.exception import FileNotFoundException


class SparkApp(Log4j, SparkSession):

    def __init__(self, config_file=None):
        """Create Spark application
    
        Parameters
        ----------
        app_name : str, default='yelp_app'
            Spark Application Name
        """
        self._config = SparkApp._get_config(config_file)
        session = SparkApp._create_session(self._config)
        super(SparkApp, self).__init__(session, session.sparkContext)
        # set application configuration
        sc = self.sparkContext
        conf = sc.getConf()
        self._id = conf.get('spark.app.id')
        self._name = conf.get('spark.app.name')
        # For temporary BigQuery export data
        self.conf.set('temporaryGcsBucket', "proj-spark")

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def config(self):
        return self._config

    @staticmethod
    def _create_session(config):
        """ """
        connectors = []
        if 'connector_paths' in config:
            connectors.append(config['connector_paths']['gcs'])
            connectors.append(config['connector_paths']['bigquery'])

        session = SparkSession \
            .builder \
            .config('spark.jars', ','.join(connectors))\
            .appName(config['app_name'])\
            .getOrCreate()

        if environ.get('APP_MODE', None) == 'circleci':
            # For GSC connector
            session._jsc.hadoopConfiguration().set(
                'fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
            session._jsc.hadoopConfiguration().set(
                'fs.gs.auth.service.account.enable', 'true')
            session._jsc.hadoopConfiguration().set(
                'google.cloud.auth.service.account.json.keyfile', 
                'google_service_account_key.json')
            # For BigQuery Connector
            session.conf.set('parentProject', config['gcp_projct'])
        return session

    @staticmethod
    def _get_config(config_file_path):
        """Application Configuration"""
        try:
            config_dict = yaml.load(open(config_file_path, mode='r'), 
                                    Loader=yaml.SafeLoader)
        except (Py4JJavaError, FileNotFoundError) as ex:
            ex = ex.java_exception if isinstance(ex, Py4JJavaError) else ex
            raise FileNotFoundException(ex, config_file_path)
        return config_dict
