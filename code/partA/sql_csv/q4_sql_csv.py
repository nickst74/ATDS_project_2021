from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("q4_sql_csv") \
    .getOrCreate()

# Read movie_genres.csv file
df1 = spark.read.options(inferSchema='True').\
        csv("hdfs://master:9000/data/movie_genres.csv")

movie_genres = df1.select(df1._c0.alias("Movie"), df1._c1.alias("Genre")).\
        createOrReplaceTempView("movie_genres")

# Read movies.csv file
df2 = spark.read.options(inferSchema='True').\
	csv("hdfs://master:9000/data/movies.csv")

movies = df2.select(df2._c0.alias("Movie"), df2._c2.alias("Summary"), df2._c3.alias("Released")).\
	createOrReplaceTempView("movies")

# optimizations are expected to be handled by SparqSQL
# so we keep the query simple and easy to undertstand
# (unlike rdd API where optimizations are up to us)
# (AVG screwed up the results so we are using sum/count)
result = spark.sql("""SELECT 5_Year_Interval, SUM(SummaryLength)/COUNT(*) as Mean_Summary_Length
			FROM movie_genres g
			INNER JOIN (	Select Movie,
						LENGTH(Summary)-LENGTH(REPLACE(Summary, ' ', ''))+1 as SummaryLength,
						CASE
							WHEN YEAR(Released) BETWEEN 2000 AND 2004 THEN 1
							WHEN YEAR(Released) BETWEEN 2005 AND 2009 THEN 2
							WHEN YEAR(Released) BETWEEN 2010 AND 2014 THEN 3
							WHEN YEAR(Released) BETWEEN 2015 AND 2019 THEN 4
							ELSE 0
							END AS 5_Year_Interval
					FROM movies
					WHERE YEAR(Released) >= 2000) m
			ON g.Movie = m.Movie
			WHERE Genre = "Drama"
			GROUP BY 5_Year_Interval
			ORDER BY 5_Year_Interval""")

result.show()
