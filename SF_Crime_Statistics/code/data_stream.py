import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


KAFKA_SERVER_URL = 'localhost:9092'
TOPIC_NAME = 'org.pdc.crimeevents'

# :: Create a schema for incoming resources
schema = StructType([StructField("offense_date", StringType(), True),
                    StructField("address_type", StringType(), True),
                    StructField("disposition", StringType(), True),
                    StructField("agency_id", StringType(), True),
                    StructField("common_location", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("call_date", StringType(), True),
                    StructField("call_date_time", StringType(), True),
                    StructField("report_date", StringType(), True),
                    StructField("crime_id", StringType(), True),
                    StructField("call_time", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("original_crime_type_name", StringType(), True)
        ])


def run_spark_job(spark):

    # :: Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_SERVER_URL) \
        .option('subscribe', TOPIC_NAME) \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', 200) \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()
    #df.show()

    # :: extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
            .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
            .select("DF.*")
    service_table.printSchema()
    
    # :: select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('disposition'))

    # count the number of original crime type
    agg_df = distinct_table \
         .withWatermark("call_datetime", "60 minutes") \
        .groupBy(
                psf.window(distinct_table.call_datetime, "10 minutes", "5 minutes"),
                distinct_table.original_crime_type_name
            ).count()
    
    # :: Q1. Submit a screen shot of a batch ingestion of the aggregation
    # :: write output stream
    query = agg_df \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # :: attach a ProgressReporter
    query.awaitTermination()

    # :: get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # :: rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    #radio_code_df.show()
    
    # :: join on disposition column
    join_query = agg_df \
                 .join(radio_code_df, col('agg_df.disposition') == col('radio_code_df.disposition'), 'left_outer')
    
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.ui.port', '3000') \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    
    # Change the log level to check output
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
