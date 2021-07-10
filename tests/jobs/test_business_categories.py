"""
Test the `BusinessCat` ETL job module.
"""

import sys

import pytest
import pandas as pd

from src.jobs import BusinessCat
from src.utils.exception import BigQueryException


class TestSuiteBusinessCat:

    def test_busineess_categories(self, resource):
        expected_data = pd.read_csv(
            resource.config['expected_data_path']
                           [__class__.__name__]
                           [sys._getframe().f_code.co_name]
        )

        job = BusinessCat(resource)
        transformed_data = job.run(action='return')

        pd.testing.assert_frame_equal(transformed_data, 
                                      expected_data,
                                      check_index_type=False, 
                                      check_dtype=False)
        
    def test_busineess_categories_bigquery_exception(self, resource):
        temporaryGcsBucket = resource.conf.get('temporaryGcsBucket', None)
        with pytest.raises(BigQueryException):
            resource.conf.set('temporaryGcsBucket', '')
            job = BusinessCat(resource)
            job.run(action='bigquery')
        resource.conf.set('temporaryGcsBucket', temporaryGcsBucket)

    def test_busineess_categories_bigquery(self, resource):
        job = BusinessCat(resource)
        assert job.run(action='bigquery') is None
