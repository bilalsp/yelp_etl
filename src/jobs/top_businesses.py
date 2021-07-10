"""
Yelp ETL Job to load top businesses to BigQuery based on number of checkin by 
customers.
"""
import pyspark.sql.functions as F
from pyspark.sql.functions import col

from src.jobs import Job


class TopBusinesses(Job):

    def __init__(self, app, *args, limit=10, **kwargs):
        self.spark = app
        self.top = limit
        paths = app.config['source_data_path']
        src_data_paths = {'checkin': paths['checkin'], 'business': paths['business']}
        table_name = app.config['jobs'][__class__.__name__]['table']
        super(TopBusinesses, self).__init__(app, 
                                            src_data_paths=src_data_paths,
                                            table_name=table_name)

    def transform(self, dataframes):
        df_checkin = dataframes['checkin']
        df_business = dataframes['business']
        n = self.top
        # checkin count for each business
        df_checkin_cnt = df_checkin.withColumn('checkin_count', 
                                               F.size(F.split(col('date'), ', ')))

        # join the checkin info with businesses data
        joinExpression = df_business['business_id'] == df_checkin_cnt['business_id']
        df_bs_checkin = df_business.join(df_checkin_cnt, joinExpression)\
                                   .drop(df_checkin_cnt['business_id'])
        df_top_bs = df_bs_checkin.orderBy(['checkin_count', 'business_id'], ascending=False)\
                                 .limit(n)

        # top businesses on yelp
        df_top_bs = df_top_bs.select('business_id', 'name', 'city', 'state', 
                                     'stars', 'checkin_count', 'review_count')
        return df_top_bs      