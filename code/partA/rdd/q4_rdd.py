from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("q1csv").getOrCreate()
sc = spark.sparkContext

# our more complex split function
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# Emit (key=movieID, ,value=(releaseYear, summaryLength)) pairs
def custom_split(x):
        tokens = split_complex(x)
        movieID = int(tokens[0])
        summary = len(tokens[2])
        year = tokens[3][0:4]
        return (movieID, (year, summary))

# Return (MovieID, Genre)
def split_genres(x):
        tokens = x.split(',')
        return (int(tokens[0]), tokens[1])

# Read csv file and emit key=MovieID and value=(realeaseYear,summaryLength).
# Filter movies released from year 2000 later
# Map ReleaseYear to the desired 5-years-intervals
# Then we emit(MovieID, (5-year-interval-ofRelease, SummaryLength))
movies = sc.textFile("hdfs://master:9000/data/movies.csv").\
                map(lambda x: custom_split(x)).\
                filter(lambda x: x[1][0] != '' and x[1][0] >= "2000").\
                map(lambda x: (x[0], ((int(x[1][0])-2000)//5, x[1][1])))

# Just read movie_genres.csv and get all movieIDs for Drama genre
# We emit (MovieID, "Drama")
drama_movies = sc.textFile("hdfs://master:9000/data/movie_genres.csv").\
                map(lambda x: split_genres(x)).\
                filter(lambda x: x[1] == "Drama")

# Join them by movieID so we get (movieID, ((relese,sumLen),"Drama"))
# Swap the keys with the release (also fix release to start from 1)
# Which means that we now have (5-year-release, (summaryLength))
# but only for the "Drama" genre movies
# At last find mean summary length by 5-year-release
rdd = movies.join(drama_movies).\
                map(lambda x: (x[1][0][0]+1, x[1][0][1])).\
                aggregateByKey((0,0),
                                lambda a,b: (a[0] + b,    a[1] + 1),
                                lambda a,b: (a[0] + b[0], a[1] + b[1])).\
                map(lambda x: (x[0], x[1][0]/x[1][1]))

# at last take that one result with the percentage and print it
for i in rdd.collect():
        print(i)
