import os
import sys
from pyspark.sql import functions
from pyspark.sql.functions import *

root_folder_path = os.path.dirname(os.getcwd())
if root_folder_path not in sys.path:
    sys.path.append(root_folder_path)

from utilities.rules import get_rules_dict
from pyspark.sql.types import *
def get_pipeline_schema():
    schema_struct = StructType(
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
    return schema_struct

def get_rules(tag):
    rules_dict = {}
    for rule in get_rules_dict():
        if rule['tag'] == tag:
            rules_dict[rule['name']] = rule['condition']
    return rules_dict