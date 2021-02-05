from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("q1csv").getOrCreate()
sc = spark.sparkContext

# our more complex split function
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

# return (RealeaseYear, (Title, Profit))
def my_emit_function(x):
        cost = int(x[5])
        income = int(x[6])
        # suppose decimal part won't matter with such values
        profit = float(((income - cost)/ cost)*100)
        return (int(x[3][0:4]), (x[1], profit))


# compare movies and return the one with the biggest profit
def most_profit(m1, m2):
        if m1[1] < m2[1]:
                return m2
        else:
                return m1


# Read csv file and infer schema.
lines = sc.textFile("hdfs://master:9000/data/movies.csv")

# split line, throw empty timestamps and 0 income/costs
# also filter out movies released before 2020
# at the end emit movies with key the year of release
rdd = lines.map(lambda x: split_complex(x)).\
        filter(lambda x: x[3] != '' and x[3][0:4] >= "2000"  and x[5] != '0' and x[6] != '0').\
        map(lambda x: my_emit_function(x))

# find best movie of every year
best_movies = rdd.reduceByKey(lambda m1, m2: most_profit(m1,m2)).sortByKey().collect()

for i in best_movies:
        print(i)
