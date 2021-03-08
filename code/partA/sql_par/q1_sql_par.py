from pyspark.sql import SparkSession
from pyspark.sql.functions import year

spark = SparkSession \
    .builder \
    .appName("q1_sql_csv") \
    .getOrCreate()

# Read movies.csv file
df = spark.read.parquet("hdfs://master:9000/data/movies.parquet")

# Create movie table for sql queries
# Filter for null timestamps and 0 incomes/costs
# Also just keep only movies from year 2000 and forwards
movies = df.filter((~df._c3.isNull()) & (df._c5 != 0) & (df._c6 != 0)).\
	select(year(df._c3).alias("Year"),
		df._c1.alias("Title"),
		(((df._c6-df._c5)/df._c5)*100).alias("Profit")).\
	createOrReplaceTempView("movies")

result = spark.sql("""SELECT m.Year, m.Title, m.Profit
			FROM movies m
			INNER JOIN (
				SELECT Year, MAX(Profit) AS Profit
				FROM movies
				WHERE Year >= 2000
				GROUP BY Year
			) p
			ON m.Profit = p.Profit AND m.Year = p.Year""")

result.sort(result.Year).show()
