from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("q2_sql_csv") \
    .getOrCreate()

# Read movies.csv file
df = spark.read.parquet("hdfs://master:9000/data/ratings.parquet")

ratings = df.select(df._c0.alias("User"), df._c2.alias("Rating")).\
	createOrReplaceTempView("ratings")

result = spark.sql("""SELECT COUNT(CASE WHEN MeanRating > 3 THEN 1 END)/COUNT(User)*100 AS Percentage
			FROM (	Select User, AVG(Rating) as MeanRating
				FROM ratings
				GROUP BY User)""")

result.show()
