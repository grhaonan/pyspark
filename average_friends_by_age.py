from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AverageFriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

lines = sc.textFile("./data/fakefriends.csv")
rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totals_by_age.mapValues(lambda x: x[0] / x[1])
for result in averageByAge.collect():
    print(result)