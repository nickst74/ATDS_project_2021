from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sys import argv

spark = SparkSession.builder.appName("test_read_parquet").getOrCreate()

sc = spark.sparkContext

df = spark.read.parquet("hdfs://master:9000/data/"+argv[1])
# something simple to test if ',' is properly contained in titles
# after conversion from csv to parquet
#df = df.filter(df._c1.contains(','))

df.printSchema()
df.show()
#print(df.count())
