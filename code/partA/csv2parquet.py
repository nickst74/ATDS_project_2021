from pyspark.sql import SparkSession
from sys import argv

spark = SparkSession.builder.appName("csv2parquet").getOrCreate()

sc = spark.sparkContext

# just grab the filename without the extension
filename = argv[1].split('.')[0]

# read csv and convert to dataframes also infering schema
df = spark.read.options(inferSchema='True').csv("hdfs://master:9000/data/" + filename + ".csv")

#print(df.count())
# print schema (optional)
df.printSchema()
# print dataframes (optional)
df.show()
# save the dataframe as a parquet file in HDFS
df.write.parquet("hdfs://master:9000/data/" + filename + ".parquet")
