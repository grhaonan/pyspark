from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import codecs


def loadMovieNames():
    move_names = {}

    with codecs.open('./data/ml-100k/u.item', 'r', encoding='iso-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            move_names[int(fields[0])] = fields[1]
    return move_names

spark = SparkSession.builder.appName("popular_movies").getOrCreate()
name_dict_broadcast = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([StructField("user_id", IntegerType(), True),
                    StructField("movie_id", IntegerType(), True),
                    StructField("rating", IntegerType(), True),
                    StructField("timestamp", LongType(), True)])


movie_df = spark.read.option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data")
movie_counts = movie_df.groupBy("movie_id").count()

def lookup_name(movie_id):
    return name_dict_broadcast.value[movie_id]

lookup_name_udf = func.udf(lookup_name)

movies_with_names = movie_counts.withColumn("movie_title", lookup_name_udf(func.col("movie_id")))

sorted_movies_with_names = movies_with_names.orderBy(func.desc("count"))

sorted_movies_with_names.show(10, False)

spark.stop()
