from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

spark = SparkSession \
    .builder \
    .appName("q5_sql_par") \
    .getOrCreate()

# Read movie_genres.csv file
df1 = spark.read.parquet("hdfs://master:9000/data/movie_genres.parquet")

movie_genres = df1.select(df1._c0.alias("Movie"), df1._c1.alias("Genre")).\
        createOrReplaceTempView("movie_genres")

# Read movies.csv file
df2 = spark.read.parquet("hdfs://master:9000/data/movies.parquet")

movies = df2.select(df2._c0.alias("Movie"), df2._c1.alias("Title"), df2._c7.alias("Popularity")).\
        createOrReplaceTempView("movies")

# Read ratings.csv file
df3 = spark.read.parquet("hdfs://master:9000/data/ratings.parquet")

ratings = df3.select(df3._c0.alias("User"), df3._c1.alias("Movie"), df3._c2.alias("Rating")).\
        createOrReplaceTempView("ratings")

# Create a table that will be used quite a lot
df4 = spark.sql("""     SELECT g.Genre, r.User, r.Movie, r.Rating
                        FROM ratings r
                        INNER JOIN movie_genres g
                        ON r.Movie = g.Movie
                """).createOrReplaceTempView("tmp")

df5 = spark.sql("""     SELECT Genre, User, COUNT(*) AS Ratings, MAX(Rating) AS MaxRating, MIN(Rating) AS MinRating
                        FROM tmp
                        GROUP BY Genre, User
                """).createOrReplaceTempView("user_ratings")


df6 = spark.sql("""     SELECT t1.Genre, User, t1.Ratings, MaxRating, MinRating
                        FROM user_ratings t1
                        INNER JOIN (    SELECT Genre, MAX(Ratings) AS Ratings
                                        FROM user_ratings
                                        GROUP BY Genre
                                ) t2
                        ON t1.Genre = t2.Genre AND t1.Ratings = t2.Ratings
                """).createOrReplaceTempView("most_ratings_user")


df7 = spark.sql("""     SELECT t.Genre, t.User, t.Ratings, m1.Title AS BestMovie, m1.Popularity AS BestMoviePop, t.MaxRating, m2.Title AS WorstMovie, m2.Popularity AS WorstMoviePop, t.MinRating
                        FROM most_ratings_user t
                        INNER JOIN tmp t1
                        ON t.Genre = t1.Genre AND t.User = t1.User AND t.MaxRating = t1.Rating
                        INNER JOIN tmp t2
                        ON t.Genre = t2.Genre AND t.User = t2.User AND t.MinRating = t2.Rating
                        INNER JOIN movies m1
                        ON t1.Movie = m1.Movie
                        INNER JOIN movies m2
                        ON t2.Movie = m2.Movie
                """)#.orderBy(["Genre", "Ratings", "MaxRating", "BestMoviePop", "MinRating", "WorstMoviePop"], ascending=[1,0,0,0,1,0]).dropDuplicates(["Genre"])

w = Window.partitionBy("Genre").orderBy(col("Ratings").desc(), col("MaxRating").desc(), col("BestMoviePop").desc(), col("MinRating"), col("WorstMoviePop").desc())

df8 = df7.withColumn("row", row_number().over(w)).where("row==1")

result = df8.select(df8.Genre, df8.User, df8.Ratings, df8.BestMovie, df8.MaxRating, df8.WorstMovie, df8.MinRating).orderBy(["Genre"], ascending=[1])

result.show(30)
