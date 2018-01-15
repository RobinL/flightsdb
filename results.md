# Job 1a - run as 'flights_a'

- 5 DPUS

```
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
```

Time taken: 12 minutes, 14 minutes

With allocated capacity 10, rather than 5: 12 minutes.

Time taken on dev endpoint with 5 DPUs:  47 minutes

# Job 1b - run as 'flights_b'

```
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
```

- 5 DPUS

Time taken: 2 minutes.  With allocated capacity 10, not 5:  2 minutes

# Job 2a - run as 'flights_a'

```
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

print(flights_csv.sample(False, 0.0000001).show())
```
10 DPUs.  24 mins
10 DPUs: 12 minutes

### Job 2b - run as 'flights_b'


```
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

flights_csv = flights.toDF()

print(flights_csv.sample(False, 0.0000001).show())
```

10 DPUs: 48 minutes
