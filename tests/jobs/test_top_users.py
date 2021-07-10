"""
Test the `TopUser` ETL job module.
"""
import sys

import pytest
import pandas as pd

from src.jobs import TopUsers
from src.utils.exception import BigQueryException


class TestSuiteTopUser:

    def test_top_users(self, resource):
        top = 15
        expected_data = pd.read_csv(
            resource.config['expected_data_path']
                           [__class__.__name__]
                           [sys._getframe().f_code.co_name]
        )
        job = TopUsers(resource, limit=top)
        transformed_data = job.run(action='return')
        pd.testing.assert_frame_equal(transformed_data, expected_data.head(top), 
                                      check_dtype=False)

    def test_top_users_bigquery_exception(self, resource):
        temporaryGcsBucket = resource.conf.get('temporaryGcsBucket', None)
        with pytest.raises(BigQueryException):
            resource.conf.set('temporaryGcsBucket', '')
            job = TopUsers(resource)
            job.run(action='bigquery')
        resource.conf.set('temporaryGcsBucket', temporaryGcsBucket)

    def test_top_users_bigquery(self, resource):
        top = 5
        job = TopUsers(resource, limit=top)
        assert job.run(action='bigquery') is None
