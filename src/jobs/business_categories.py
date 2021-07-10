"""
Yelp ETL Job to load business categories to BigQuery.
"""
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from src.jobs import Job


class BusinessCat(Job):

    def __init__(self, app, *args, **kwargs):
        self.spark = app
        paths = app.config['source_data_path']
        src_data_paths = {'business': paths['business']}
        table_name = app.config['jobs'][__class__.__name__]['table']
        super(BusinessCat, self).__init__(app, 
                                          src_data_paths=src_data_paths,
                                          table_name=table_name)

    def transform(self, dataframes):
        df_business = dataframes['business']
        # split categories
        df_business = df_business.transform(BusinessCat.splitCategories())
        # business categories sorted by count
        cat_grp = df_business.groupBy('category')\
            .agg(F.count('business_id').alias('business_count'))\
            .orderBy(['business_count', 'category'], ascending=False)
        return cat_grp

    @staticmethod
    def splitCategories() -> Callable[[SparkDataFrame], SparkDataFrame]:
        def _(df):
            df_res = df.withColumn('categories_splitted', F.split(F.col('categories'), ', '))\
                       .withColumn('category', F.explode('categories_splitted'))
            return df_res
        return _        