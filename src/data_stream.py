import logging
import json

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf.crime.statistics.topic") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 1000) \
        .option("maxOffsetsPerTrigger", 2000) \
        .option("stopGracefullyOnShutdown", True) \
        .load()

    # Show schema for the incoming resources for checks
    
    #print("Schema display")
    
    df.printSchema()
    #schema = df.schema
    #print(schema)
    
    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    #df.selectExpr("*", "(ColumnName AS customName)")
    #kafka_df = df.selectExpr("*", "CAST(value as STRING)")
    kafka_df = df.selectExpr("CAST(value as STRING)")
    #print("Got here")
    
    schema =  StructType([StructField("crime_id",StringType(), True),
                StructField("original_crime_type_name",StringType(), True), 
                StructField("report_date",StringType(), True),
                StructField("call_date",StringType(), True),
                StructField("offense_date",StringType(), True),
                StructField("call_time",StringType(), True),
                StructField("call_date_time",TimestampType(), True),
                StructField("disposition",StringType(), True),
                StructField("address",StringType(), True),
                StructField("city",StringType(), True),
                StructField("state",StringType(), True),
                StructField("agency_id",StringType(), True),
                StructField("address_type",StringType(), True),
                StructField("common_location",StringType(), True)
              ])
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    #print("Got here2")
    
    distinct_table = service_table.select("original_crime_type_name","disposition")
    
    #print("Got here3")
    # count the number of original crime type
    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    agg_df = (distinct_table.groupby("original_crime_type_name", "disposition").count().sort("count", ascending=False)
             )
    
    #print("Got here4")
    
    query_df_writer = agg_df \
                      .writeStream \
                      .queryName("query_df_writer")\
                      .outputMode("Complete") \
                      .format("console") \
                      .start()

    #print("Got here5")
    # TODO attach a ProgressReporter
    query_df_writer.awaitTermination()

    #print("Got here6")
    # TODO get the right radio code json path
    #radio_code_json_filepath = "radio_code.json"
    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    
    radio_code_schema =  StructType([StructField("disposition_code",StringType(), False),
                StructField("description",StringType(), True)
              ])
    
    radio_code_df = spark.read \
                              .option("multiline", True) \
                              .schema(radio_code_schema) \
                              .json(radio_code_json_filepath)

    #print("Got here7")
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    radio_code_df.printSchema()
    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
   
    radio_code_df.printSchema()
    #print("Got here8") 
    # TODO join on disposition column
    join_query_df = agg_df.join(radio_code_df, "disposition")

    #print("Got here9")
    join_query_df_writer = join_query_df \
                           .writeStream \
                           .format("console") \
                           .start()
    
    #print("Got here9a")
    join_query_df_writer.awaitTermination()
    #print("Got here10")

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
