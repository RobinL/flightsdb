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
print(flights_csv.show(10))
