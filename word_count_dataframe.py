from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("word_count").getOrCreate()

input_df = spark.read.text("./data/book.txt")

# Conver a list of words into a new row as row is the basic unit of DataFrame.
words = input_df.select(func.explode(func.split(input_df.value, "\\W+")).alias("word"))

words.filter(words.word != "")

lowercase_words = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercase_words.groupBy("word").count()

wordCounts.sort(func.desc("count")).show()

spark.stop()