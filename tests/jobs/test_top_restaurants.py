"""
Test the `TopBusinesses` ETL job module.
"""

import sys

import pytest
import pandas as pd

from src.jobs import TopRestaurants
from src.utils.exception import BigQueryException


class TestSuiteTopRestaurants:

    def test_top_restaurants(self, resource):
        top = 15
        expected_data = pd.read_csv(
            resource.config['expected_data_path']
                           [__class__.__name__]
                           [sys._getframe().f_code.co_name]
        ).sort_values(by=['checkin_count', 'business_id'], ascending=False).head(top)
        job = TopRestaurants(resource, limit=top)
        transformed_data = job.run(action='return')
        pd.testing.assert_frame_equal(transformed_data, expected_data,
                                      check_index_type=False, 
                                      check_dtype=False)
        
    def test_top_restaurants_bigquery_exception(self, resource):
        temporaryGcsBucket = resource.conf.get('temporaryGcsBucket', None)
        with pytest.raises(BigQueryException):
            resource.conf.set('temporaryGcsBucket', '')
            job = TopRestaurants(resource)
            job.run(action='bigquery')
        resource.conf.set('temporaryGcsBucket', temporaryGcsBucket)

    def test_top_restaurants_bigquery(self, resource):
        top = 5
        job = TopRestaurants(resource, limit=top)
        assert job.run(action='bigquery') is None
