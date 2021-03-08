from pyspark.sql import SparkSession
from sys import argv
from io import StringIO
import csv

spark = SparkSession.builder.appName("rdd_partition_join").getOrCreate()
spark.conf.set("spark.driver.maxResultSize", "10G")
spark.conf.set("spark.executor.memory", "2G")
spark.conf.set("spark.driver.memory", "6G")
sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

# a string with fields separated by ',' and the position of the key
# emit -> (key, [(value,tag)])
def custom_split_emit(x, pos, tag):
        tokens = split_complex(x)
        key = tokens[pos-1]
        val = tuple(tokens)
        return (key, [(val, tag)])

# just emit all possible/valid combinations (depending on tags) in the list
def custom_join(x):
        acc = []
        for i in x[1]:
                if i[1] == 1:
                        for j in x[1]:
                                if j[1] == 2:
                                        acc.append((j[0],i[0])) # (rating, genre) pair
        return (x[0], acc)

##########################################################################################
# argv = ["rdd_broadcast_join.py", "small_file", "key1_index", "big_file", "key2_index"] #
##########################################################################################

# read first file
first = sc.textFile("hdfs://master:9000/data/"+argv[1]).\
                map(lambda x: custom_split_emit(x, int(argv[2]), tag=1))

# read second file and emit key-line pairs like we did with the other one
second = sc.textFile("hdfs://master:9000/data/"+argv[3]).\
                map(lambda x: custom_split_emit(x, int(argv[4]), tag=2))

# combine the two rdds (take the union and then proceed to reduceByKey phase)
# first we need to organize them in list by their key
combined = first.union(second).reduceByKey(lambda a, b: a + b)
# then for every key(movieId) we need to emit all (movie,genre)-(movie_rating) combinations, and check for empty values at the end
final = combined.map(lambda x: custom_join(x)).filter(lambda x: len(x[1])>0)

result = final.collect()

# print result (deactivated for obvious reasons)
#for i in result:
#       print(i)
