from pyspark.sql import SparkSession
from sys import argv
import csv

spark = SparkSession.builder.appName("rdd_broadcast_join").getOrCreate()
sc = spark.sparkContext

# a string with fields separated by ',' and the position of the key
def custom_split_emit(x, pos, inList=False):
        tokens = x.split(',')
        key = tokens[pos-1]
        if inList:
                val = [tuple(tokens)]
        else:
                val = tuple(tokens)
        return (key, val)

# for every item check if key exists in local HashMap and join it, else we will discard it
def custom_join(x):
        key = x[0]
        if key in HashMap.keys():
                acc = []
                for v in HashMap[key]:
                        acc.append((x[1],v))
                return (key, acc)
        else:
                return ('-1', None)

##########################################################################################
# argv = ["rdd_broadcast_join.py", "small_file", "key1_index", "big_file", "key2_index"] #
##########################################################################################

# read first file (we expect it to be the smaller one, which we broadcast and create the HashMap)
small = sc.textFile("hdfs://master:9000/data/"+argv[1]).\
                map(lambda x: custom_split_emit(x, int(argv[2]),inList=True)).\
                reduceByKey(lambda a, b: a + b)

# broadcast HashMap to all workers
broadcastVar = sc.broadcast(small.collectAsMap())
HashMap = broadcastVar.value

# read big file and emit key-line pairs like we read with the other one
big = sc.textFile("hdfs://master:9000/data/"+argv[3]).\
                map(lambda x: custom_split_emit(x, int(argv[4])))

joined = big.map(lambda x: custom_join(x)).filter(lambda x: x[0] != '-1').reduceByKey(lambda a, b: a + b)
result = joined.collect()

# print result (deactivated for obvious reasons)
#for i in result:
#       print(i)

# destroy broadcasted variable to free to the space on all workers
broadcastVar.destroy()
