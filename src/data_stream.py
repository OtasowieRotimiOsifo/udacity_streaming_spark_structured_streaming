import logging
import json

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema_items = [StructField("crime_id": StringType(), true),
                StructField("original_crime_type_name": StringType(), true), 
                StructField("report_date": StringType(), true),
                StructField("call_date": StringType(), true),
                StructField("offense_date": StringType(), true),
                StructField("call_time": StringType(), true),
                StructField("call_date_time": TimestampType(), true),
                StructField("disposition": StringType(), true),
                StructField("address": StringType(), true),
                StructField("city": StringType(), true),
                StructField("state": StringType(), true),
                StructField("agency_id": StringType(), true),
                StructField("address_type": StringType(), true),
                StructField("common_location": StringType(), true)
              ]
schema = StructType(schema_items)

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf.crime.statistics.spark.streaming") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    //df.selectExpr("*", "(ColumnName AS customName)")
    kafka_df = df.selectExpr("*", "CAST(value as STRING)")
    #kafka_df = df.selectExpr(CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = \
    service_table.select("original_crime_type_name", \
                        "call_date_time", \
                        "disposition").withWatermark("call_time", "60 miuntes")

    # count the number of original crime type
    agg_df = distinct_table.groupby("original_crime_type_name", psf.window("call_time", "60 miuntes")).count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query_df_writer = agg_df \
                      .writeStream \
                      .queryName("agg_query_df_writer") \
                      .outputModel("Complete") \
                      .format("console") \
                      .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query_df = agg_df.join(radio_code_df, "disposition")

    join_query_df_writer = join_query_df \
                           .writeStream \
                           .queryName("join_query_df_writer") \
                           .outputModel("append") \
                           .format("console") \
                           .start()
    
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
