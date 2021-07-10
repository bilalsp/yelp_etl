"""
Yelp ETL Job to load top users to BigQuery based on number of reviews written by 
user.
"""
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql import DataFrame as SparkDataFrame

from src.jobs import Job


class TopUsers(Job):

    def __init__(self, app, *args, limit=10, **kwargs):
        self.spark = app
        self.top = limit
        paths = app.config['source_data_path']
        src_data_paths = {'review': paths['review'], 'user': paths['user']}
        table_name = app.config['jobs'][__class__.__name__]['table']
        super(TopUsers, self).__init__(app, 
                                       src_data_paths=src_data_paths,
                                       table_name=table_name)

    def transform(self, dataframes):
        """Apply transformation on dataframes to get top users"""
        df_review = dataframes['review']
        df_user = dataframes['user']
        n = self.top
        df_ntop_users = df_review.transform(TopUsers.aggregateByUserID())\
                                 .transform(TopUsers.ntop(n))\
                                 .transform(TopUsers.addUserName(df_user))\
                                 .transform(TopUsers.rearrangeCol())
        return df_ntop_users

    @staticmethod
    def aggregateByUserID() -> Callable[[SparkDataFrame], SparkDataFrame]:
        def _(df):
            df_res = df.groupBy('user_id')\
                .agg(F.count('review_id').alias('#reviews'),
                     F.min(col('date')).alias('min_date'),
                     F.max(col('date')).alias('max_date'),
                     F.sum(col('useful')).alias('sum_useful'),
                     F.sum(col('funny')).alias('sum_funny'),
                     F.sum(col('cool')).alias('sum_cool'),
                     F.round(F.mean(col('stars')), 3).alias('avg_stars'))
            return df_res
        return _

    @staticmethod
    def ntop(n: int = 10) -> Callable[[SparkDataFrame], SparkDataFrame]:
        def _(df):
            df_res = df.orderBy(F.desc('#reviews'), F.asc('user_id'))\
                       .withColumn('rank', F.monotonically_increasing_id() + F.lit(1))\
                       .limit(n)
            return df_res
        return _

    @staticmethod
    def addUserName(df_user) -> Callable[[SparkDataFrame], SparkDataFrame]:
        def _(df):
            joinExpression = df_user.user_id == df.user_id
            df_res = df_user.select('user_id', 'name')\
                .join(F.broadcast(df), joinExpression, 'right_outer')\
                .drop(df_user.user_id)\
                .orderBy(F.desc('#reviews'), F.asc('user_id'))
            return df_res
        return _

    @staticmethod
    def rearrangeCol() -> Callable[[SparkDataFrame], SparkDataFrame]:
        def _(df):
            new_cols = ['rank', 'user_id', 'name'] + df.columns[2:-1]
            df_res = df.select(new_cols)
            return df_res
        return _        