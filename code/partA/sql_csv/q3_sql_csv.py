from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("q3_sql_csv") \
    .getOrCreate()

# Read movie_genres.csv file
df1 = spark.read.options(inferSchema='True').\
        csv("hdfs://master:9000/data/movie_genres.csv")

movie_genres = df1.select(df1._c0.alias("Movie"), df1._c1.alias("Genre")).\
        createOrReplaceTempView("movie_genres")

# Read ratings.csv file
df2 = spark.read.options(inferSchema='True').\
	csv("hdfs://master:9000/data/ratings.csv")

ratings = df2.select(df2._c1.alias("Movie"), df2._c2.alias("Rating")).\
	createOrReplaceTempView("ratings")

# AVG seems to have a small difference on the result but it happens after
# the 14th decimal digit, so it feels like it never happened XD
result = spark.sql("""SELECT Genre, AVG(Rating) as Mean_Rating, COUNT(m.Movie) as Total_Movies
			FROM movie_genres m
			INNER JOIN (	Select Movie, AVG(Rating) as Rating
					FROM ratings
					GROUP BY Movie) r
			ON m.Movie = r.Movie
			GROUP BY m.Genre
			ORDER BY Genre ASC""")

result.show()
