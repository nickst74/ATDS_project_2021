from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("q2_rdd").getOrCreate()
sc = spark.sparkContext

# our more complex split function
def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

# Read csv file and split, and emit key=userID and value=rating.
# every user has at most one rating for a movie (already checked that :) )
# emit (userID, rating) pairs and then find mean of ratings for every user
# then find the percentage of users with mean ratings bigger than 3
# only need tuple for every mean rating as it relates to one user exctly
# suppose only count users that actually rated something
rdd = sc.textFile("hdfs://master:9000/data/ratings.csv").\
		map(lambda x: x.split(',')).\
		map(lambda x: (int(x[0]), float(x[2]))).\
		aggregateByKey((0,0),
				lambda a,b: (a[0] + b,    a[1] + 1),
				lambda a,b: (a[0] + b[0], a[1] + b[1])).\
		map(lambda x: (1, x[1][0]/x[1][1])).\
		aggregateByKey((0,0),
				lambda a,b: (a[0]+1,a[1]+1) if b>3 else (a[0], a[1]+1),
				lambda a,b: (a[0]+b[0], a[1]+b[1])).\
		map(lambda x: (100*x[1][0]/x[1][1]))

# at last take that one result with the percentage and print it
for i in rdd.take(1):
	print("The answer is: " + str(i) + " %")
	#print(i)
