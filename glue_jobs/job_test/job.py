import os
import sys
import urllib
import csv

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

flights_csv = spark.read.csv("s3://robintest-gluenotebooks3files/flights", header=True)
print(flights_csv.take(5))
print(flights_csv.show(2))

print(flights_csv.sample(False, 0.0000001).show())

flights_csv.registerTempTable('flights_csv')

sql = "select * from flights_csv limit 10"
sql_results = spark.sql(sql)
print(sql_results.show())

flights = glueContext.create_dynamic_frame.from_catalog(
             database="flights",
             table_name="flightsdata")
flights_df = flights.toDF()

print(flights_df.show(10))
