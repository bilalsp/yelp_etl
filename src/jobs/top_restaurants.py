"""
Yelp ETL Job to load top restaurants to BigQuery based on number of checkin by 
customers.
"""
import pyspark.sql.functions as F

from src.jobs import Job


class TopRestaurants(Job):

    def __init__(self, app, *args, limit=10, **kwargs):
        self.spark = app
        self.top = limit
        paths = app.config['source_data_path']
        src_data_paths = {'checkin': paths['checkin'], 'business': paths['business']}
        table_name = app.config['jobs'][__class__.__name__]['table']
        super(TopRestaurants, self).__init__(app, 
                                             src_data_paths=src_data_paths,
                                             table_name=table_name)

    def transform(self, dataframes):
        df_checkin = dataframes['checkin']
        df_business = dataframes['business']
        n = self.top
        state = self.spark.config['jobs'][__class__.__name__]['state']
        # Restaurants in specified state
        df_restaurant = df_business.filter(
            F.lower(F.col('categories')).contains('res'))\
            .filter(F.col('state') == state)
        # checkin count for each business
        df_checkin_cnt = df_checkin.withColumn('checkin_count', 
                                               F.size(F.split(F.col('date'), ', ')))

        # join the checkin info with businesses data
        joinExpression = df_restaurant['business_id'] == df_checkin_cnt['business_id']
        df_rest_checkin = df_restaurant.join(df_checkin_cnt, joinExpression)\
                                       .drop(df_checkin_cnt['business_id'])
        df_top_rest = df_rest_checkin.orderBy(['checkin_count', 'business_id'], ascending=False)\
                                     .limit(n)

        # top businesses on yelp
        df_top_rest = df_top_rest.select('business_id', 'name', 'city', 'state', 
                                         'stars', 'checkin_count', 'review_count')

        return df_top_rest
