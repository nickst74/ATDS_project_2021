from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("q1csv").getOrCreate()
sc = spark.sparkContext

# our more complex split function
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# Read ratings.csv file and split, and emit key=MovieID and value=rating.
# emit (MovieID, rating) pairs and find the mean rating for every movie
movies = sc.textFile("hdfs://master:9000/data/ratings.csv").\
                map(lambda x: x.split(',')).\
                map(lambda x: (int(x[1]), float(x[2]))).\
                aggregateByKey((0,0),
                                lambda a,b: (a[0] + b,    a[1] + 1),
                                lambda a,b: (a[0] + b[0], a[1] + b[1])).\
                map(lambda x: (x[0], x[1][0]/x[1][1]))

# Read movie_genres.csv and emit (key=MovieID,value=Genre)
# could be done in one step with a custom function but naaah
genres = sc.textFile("hdfs://master:9000/data/movie_genres.csv").\
                map(lambda x: x.split(',')).\
                map(lambda x: (int(x[0]), x[1]))

# Join the two rdds by key (= MovieID )
# So we now have (MovieID, (Rating, Genre))
# Swap key from movie id to Genre
# so we emit (Genre, Rating)
# Then for every genre find sum of movies
# along with mean rating (for the second we need
# to calculate the sum of movies' mean ratings)
genre_rating = movies.join(genres).\
                map(lambda x: (x[1][1], x[1][0])).\
                aggregateByKey((0,0),
                                lambda a,b: (a[0] + b,    a[1] + 1),
                                lambda a,b: (a[0] + b[0], a[1] + b[1])).\
                map(lambda x: (x[0], x[1][0]/x[1][1], x[1][1]))


# Print the results
for i in genre_rating.collect():
        print(i)
