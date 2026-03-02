import sys
import os

root_folder_path = os.path.dirname(os.getcwd())

if root_folder_path not in sys.path:
    sys.path.append(root_folder_path)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.testing import assertSchemaEqual
from utilities.utils import get_pipeline_schema


def test_get_pipeline_schema_matches():
    expected_schema = StructType(
        [
        StructField('VendorID', IntegerType()),
        StructField('tpep_pickup_datetime', TimestampType()),
        StructField('tpep_dropoff_datetime', TimestampType()),
        StructField('passenger_count', LongType()),
        StructField('trip_distance', DoubleType()),
        StructField('RatecodeID', LongType()),
        StructField('store_and_fwd_flag', StringType()),
        StructField('PULocationID', IntegerType()),
        StructField('DOLocationID', IntegerType()),
        StructField('payment_type', LongType()),
        StructField('fare_amount', DoubleType()),
        StructField('extra', DoubleType()),
        StructField('mta_tax', DoubleType()),
        StructField('tip_amount', DoubleType()),
        StructField('tolls_amount', DoubleType()),
        StructField('improvement_surcharge', DoubleType()),
        StructField('total_amount', DoubleType()),
        StructField('congestion_surcharge', DoubleType()),
        StructField('Airport_fee', DoubleType()),
        StructField('cbd_congestion_fee', DoubleType())
        ]
    )
    actual_schema = get_pipeline_schema()
    assertSchemaEqual(actual_schema, expected_schema)

import pytest
def test_get_rules_returnsExpectedDict(monkeypatch):
    from utilities.utils import get_rules
    fake_rules = [
            {'name': 'test_name_1',
            'condition': 'rule_1',
            'tag': 'tag_1'},
            {
                'name': 'test_name_2',
                'condition': 'rule_2',
                'tag': 'tag_1'
            },
            {
                'name': 'test_name_3',
                'condition': 'rule_3',
                'tag': 'tag_2'
            },
            {
                'name': 'test_name_4',
                'condition': 'rule_4',
                'tag': 'tag_3'
            }
        ]
    def fake_get_dict():
        return fake_rules
    from utilities import utils
    monkeypatch.setattr(utils, 'get_rules_dict', fake_get_dict)

    expected_dict = {
        'test_name_1': 'rule_1',
        'test_name_2': 'rule_2'
    }

    actual_dict = get_rules('tag_1')

    assert expected_dict == actual_dict
