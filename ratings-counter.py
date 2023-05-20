from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# This is quite an older way, modern way is to use SparkSession which already includes SparkContext, SQLContext,
# and HiveContext automatically.
sc = SparkContext(conf=conf)

lines = sc.textFile("./data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sorted_results = collections.OrderedDict(sorted(result.items()))
for key, value in sorted_results.items():
    print("%s %i" % (key, value))