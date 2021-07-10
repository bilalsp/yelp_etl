"""
Every Yelp ETL Job class should extend this abstract `Job` class and must implement
`tranform` abstract method.
"""
from abc import ABC, abstractmethod
from py4j.protocol import Py4JJavaError

from pyspark.sql import DataFrame as SparkDataFrame

from src.utils.validation import validate_action
from src.utils.exception import BigQueryException


class Job(ABC):

    def __init__(self, app, *args, **kwargs):
        self.spark = app
        self.table_name = kwargs['table_name']
        self.src_data_paths = kwargs['src_data_paths']
        super(Job, self).__init__()

    @property
    def name(self):
        return self.__class__.__name__

    @abstractmethod
    def transform(self, dataframes):
        raise NotImplementedError

    @validate_action
    def run(self, action='BigQuery'):
        """Run the ETL job."""
        # Extract Data
        dataframes = self.extract_data()
        # Transform Data
        df_transformed = self.transform(dataframes)
        # Load Data
        response = self.load_data(df_transformed, action)
        return response

    def extract_data(self):
        """Extract the data from google cloud storage."""
        spark = self.spark
        spark_dataframes = {}
        for name, url in self.src_data_paths.items():
            df = spark.read.format('json').load(url)
            spark_dataframes[name] = df
        return spark_dataframes 

    def load_data(self, df: SparkDataFrame, action: str):
        """Load the transformed data to BigQuery."""
        if action.lower() == 'return':
            return df.toPandas()

        try:
            df.write.format('bigquery') \
                .option('table', self.table_name) \
                .mode("append") \
                .save()
        except (Exception, Py4JJavaError) as ex:
            ex = ex.java_exception if isinstance(ex, Py4JJavaError) else ex
            raise BigQueryException(ex, __class__.__name__)
