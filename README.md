# Task3-PySpark-JSON-Data-Example

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("jsondata").getOrCreate()

data = [
     {"Id": 1, "Name":"John","Age": 30},
     {"Id": 2, "Name":"Mike","Age": 40},
     {"Id": 3, "Name":"Anna","Age": 50},
     {"Id": 4, "Name":"Tom","Age": 35}

]
 # Create DataFrame
df = spark.createDataFrame(data) 

# Save DataFrame as JSON
df.write.json("/FileStore/tables/device4.json")

data1 =spark.read.format("json").option("multiline","true").load("/FileStore/tables/device4.json")

data1.show()
