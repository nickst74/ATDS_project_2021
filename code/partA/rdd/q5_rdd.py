from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("q1csv").getOrCreate()
sc = spark.sparkContext

# our more complex split function
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# emit(movieID, (title, popularity))
def split_movies(x):
        tokens = split_complex(x)
        movie_id = int(tokens[0])
        title = tokens[1]
        popularity = float(tokens[7])
        return (movie_id, (title, popularity))

# emit(movieID, genre)
def split_genres(x):
        tokens = x.split(',')
        return (int(tokens[0]), tokens[1])

# emit(movieID, (user, rating))
def split_ratings(x):
        tokens = x.split(',')
        user = int(tokens[0])
        movie_id = int(tokens[1])
        rating = float(tokens[2])
        return (movie_id, (user, rating))

def my_min(x, y):
        if x[0] != y[0]:
                return min(x,y)
        elif x[1] < y[1]:
                return y
        else:
                return x

# Read movie genres and keep movie_id as key
genres = sc.textFile("hdfs://master:9000/data/movie_genres.csv").\
                map(lambda x: split_genres(x))

# Get (MovieID, ((Title, Popularity), Genre)) after join on
# genres and movies files on movie_id key
# emits (movieID, ((title, popularity), genre))
movies = sc.textFile("hdfs://master:9000/data/movies.csv").\
                map(lambda x: split_movies(x)).\
                join(genres)

# Now that we have all movie details needed join
# movie details with ratings by movieID
# after join we get (movieID, ((user, rating), ((title, popularity), genre)))
# also sample from execution to make sure of that
# (15, ((26692, 3.0), (('Citizen Kane', 15.811921), 'Mystery')))
# emit ((genre, user), (count, (rating, popularity, title), (rating, popularity, title)))
# (('Comedy', 177705), (1, (5.0, 1.732983, 'Dialogue avec mon jardinier'), (-1, 0, '')))
genre_user_ratings = sc.textFile("hdfs://master:9000/data/ratings.csv").\
                map(lambda x: split_ratings(x)).\
                join(movies).\
                map(lambda x: ((x[1][1][1], x[1][0][0]),
                                (x[1][0][1], x[1][1][0][1], x[1][1][0][0]))).\
                aggregateByKey((0, (-1,0, ''), (11,0, '')), # (ratings_count, best_movie, worst_movie)
                                lambda a, b: (a[0]+1, max(a[1],b), my_min(a[2], b)),
                                lambda a, b: (a[0]+b[0], max(a[1], b[1]), my_min(a[2], b[2])))

# Get best user for each genre by getting genre as key
# and performing reduce for max rating count
result = genre_user_ratings.map(lambda x: (x[0][0],
                        (x[0][1], x[1][0], x[1][1][2], x[1][1][0], x[1][2][2], x[1][2][0]))).\
                        reduceByKey(lambda a, b: b if a[1] < b[1] else a).\
                        sortByKey().\
                        collect()


#print(rdd.count())
print("{:^20}|{:^10}|{:^10}|{:^50}|{:^10}|{:^50}|{:^10}".format('Genre', 'User', 'Count', 'Best Movie', 'Rating', 'Worst Movie', 'Rating'))
print("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")
for i in result:
        print("{:^20}|{:^10}|{:^10}|{:^50}|{:^10}|{:^50}|{:^10}".format(i[0], i[1][0], i[1][1], i[1][2][0:50], i[1][3], i[1][4][0:50], i[1][5]))
        #print(i)
