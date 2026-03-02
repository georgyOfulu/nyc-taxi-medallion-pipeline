from pyspark import pipelines as dp
from pyspark.sql import functions
from pyspark.sql.functions import *
from utilities import utils
from utilities.utils import get_rules, get_pipeline_schema

quality_expecations = get_rules('validity')
@dp.table(
    comment = 'data ingested from an s3 bucket',
    name = 'trips_bronze_1'
)
def trips_bronze_1():
    data_location = spark.conf.get('data_location')
    schema_ = spark.conf.get('schema')
    table_schema = get_pipeline_schema()
    return(
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'parquet')
        .option('cloudFiles.schemaLocation', f'{data_location}/{schema_}')
        .option('schema', table_schema)
        .option('mergeSchema', 'false')
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
        .option('cloudFiles.inferColumnTypes', 'false')
        .option('rescueDataColumn', 'error_files_to_inspect')
        .option("cloudFiles.schemaHints", "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP")
        .load(data_location)
        .select(
            '*',
            col('_metadata.file_name').alias('source_file_name')
        )
    )
@dp.table(
    comment = 'payments lookup data',
    name = 'payments_lookup'
)
def payments_lookup():
    return(
        spark.read.table('payments')
    )

dp.create_streaming_table(
    comment = 'implements type II scd logic to the payments lookup table',
    name = 'payments_lookup_scd'
)

dp.create_auto_cdc_from_snapshot_flow(
    target = 'payments_lookup_scd',
    source = 'payments_lookup',
    keys = ['payment_type'],
    stored_as_scd_type = 2
)

@dp.table(
    comment = 'data quality checks and transformations',
    name = 'trips_silver_1'
)
@dp.expect_all(quality_expecations)
def trips_silver_1():
    return(
        spark.readStream.table('trips_bronze_1')
        .select(
            'VendorID',
            'payment_type',
            'total_amount',
            date_format('tpep_dropoff_datetime', 'MMMM').alias('trip_month')
        )
    )

@dp.table(
    comment = 'joins payment_type to the payment type lookup table',
    name = 'trips_silver_joined'
)

def trips_silver_joined():
    return(
        spark.readStream.table('trips_silver_1')
        .join(dp.read('payments_lookup_scd'),
              on = 'payment_type',
              how = 'left')
    )


@dp.materialized_view(
    comment = 'aggregated data for reporting'
)
def trips_gold_1():
    return(
        dp.read('trips_silver_joined')
        .groupBy('trip_month', 'payment_type', 'payment_description')
        .agg(sum('total_amount').alias('total_amount'))
    )