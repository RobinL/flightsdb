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

flights = glueContext.create_dynamic_frame.from_catalog(
             database="flights",
             table_name="flightsdata")

flights_df = flights.toDF()

print(flights_df.show(10))

