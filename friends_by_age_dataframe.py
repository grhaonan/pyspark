from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("friends_by_age").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/fakefriends-header.csv")

friends_by_age = lines.select("age", "friends")

friends_by_age.groupBy("age").avg("friends").sort("age").show()

friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()