from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType


spark = SparkSession.builder.appName("popular_movies").getOrCreate()

schema = StructType([StructField("user_id", IntegerType(), True),
                     StructField("movie_id", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])

movie_df = spark.read.option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data")

top_movie_ids = movie_df.groupBy("movie_id").count().orderBy(func.desc("count"))

top_movie_ids.show(10)

spark.stop()