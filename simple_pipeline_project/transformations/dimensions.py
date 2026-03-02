from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

payment_data = [
    (1, 'Credit Cards'),
    (2, 'Cash'),
    (3, 'No Charge'),
    (4, 'Dispute'),
    (5, 'Voided Trip'),
    (6, 'Unknown')
    ]

payment_df = spark.createDataFrame(payment_data, ['payment_type', 'payment_description'])

payment_df.write.format('delta').mode('overwrite').saveAsTable('best_pipeline.payments')


