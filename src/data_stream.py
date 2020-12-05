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
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", True) \
        .load()

    # Show schema for the incoming resources for checks
    
    df.printSchema()
    
    # Take only value and convert it to String
    #kafka_df = df.selectExpr("CAST(value as STRING)")
    kafka_df = df.selectExpr("CAST(timestamp as STRING)", "CAST(value as STRING)")
    
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

    #distinct_table = service_table.select("original_crime_type_name","disposition")
    distinct_table = service_table.select("original_crime_type_name","call_date_time", "disposition").withWatermark("call_date_time", "45 minutes")
    # count the number of original crime type
    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    #agg_df = (distinct_table.groupby("original_crime_type_name",       "disposition").count().sort("count", ascending=False))
    agg_df = distinct_table.groupby("original_crime_type_name",       psf.window("call_date_time", "45 minutes")).count()#.sort("count", ascending=False))
    
    query_df_writer = agg_df \
                      .writeStream \
                      .queryName("query_df_writer")\
                      .outputMode("Complete") \
                      .format("console") \
                      .start() \
                      .awaitTermination()
                      
    #query_df_writer.awaitTermination()
    
    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    
    radio_code_schema =  StructType([StructField("disposition_code",StringType(), False),
                StructField("description",StringType(), True)
              ])
    
    radio_code_df = spark.read \
                              .option("multiline", True) \
                              .schema(radio_code_schema) \
                              .json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
   
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
   
    join_query_df = agg_df.join(radio_code_df, "disposition")

    join_query_df_writer = join_query_df \
                           .writeStream \
                           .format("console") \
                           .start()\
                           .awaitTermination()  
    
    #join_query_df_writer.awaitTermination()
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .config("spark.sql.shuffle.partitions", 10) \
        .config("spark.default.parallelism", 10000) \
        .config("spark.kafka.streaming.maxRatePerPartition", 10) \
        .getOrCreate()
        
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
