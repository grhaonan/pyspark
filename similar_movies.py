from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

# cosine_similarity(Movie A, Movie B) = dot_product(Movie A, Movie B) / (norm(Movie A) * norm(Movie B))
def compute_cosine_similarity(spark, data):

    pair_scores = movie_pairs.withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    calculate_similarity = pair_scores.groupBy("movie1", "movie2").agg(func.sum(func.col("xy")).alias("numerator"), \
                                                                       (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
                                                                       func.count(func.col("xy")).alias("numPairs")
                                                                       )

    result = calculate_similarity.withColumn("score",
                                             func.when(func.col("denominator") !=0, func.col("numerator") / func.col("denominator")) \
                                             .otherwise(0)
                                             ).select("movie1", "movie2", "score", "numPairs")
    return result


spark = SparkSession.builder.appName("similar_movies").master("local[*]").getOrCreate()

movie_names_schema = StructType([StructField("movie_id", IntegerType(), True),
                                StructField("movie_title", StringType(), True)])

movies_schema = StructType([StructField("user_id", IntegerType(), True),
                            StructField("movie_id", IntegerType(), True),
                            StructField("rating", IntegerType(), True),
                            StructField("timestamp", IntegerType(), True)])

movie_names = spark.read.option("sep", "|").option("charset", "ISO-8859-1").schema(movie_names_schema).csv("./data/ml-100k/u.item")


movies = spark.read.option("sep", "\t").schema(movies_schema).csv("./data/ml-100k/u.data")


ratings = movies.select("user_id", "movie_id", "rating")

movie_pairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.user_id") == func.col("ratings2.user_id"))
          & (func.col("ratings1.movie_id") < func.col("ratings2.movie_id"))) \
    .select(func.col("ratings1.movie_id").alias("movie1"), \
            func.col("ratings2.movie_id").alias("movie2"), \
            func.col("ratings1.rating").alias("rating1"), \
            func.col("ratings2.rating").alias("rating2"))

movie_pairs_similarity = compute_cosine_similarity(spark, movie_pairs).cache()


def get_movie_name(movie_names, movie_id):
    # this will be Row() object
    result = movie_names.filter(func.col("movie_id") == movie_id).select("movie_title").collect()[0]
    # this will return the value of Row() object
    return result[0]


if len(sys.argv) > 1:
    score_threshold = 0.97
    co_occurrence_threshold = 50

    movie_id = int(sys.argv[1])

    filtered_results = movie_pairs_similarity.filter(((func.col("movie1") == movie_id) | (func.col("movie2") == movie_id)) & (func.col("score") > score_threshold) & (func.col("numPairs") > co_occurrence_threshold))

    results = filtered_results.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + get_movie_name(movie_names, movie_id))

    for result in results:
        similar_movie_id = result.movie1
        if similar_movie_id == movie_id:
            similar_movie_id = result.movie2

        print(get_movie_name(movie_names, similar_movie_id) + "\tscore: " + str(result.score) + "\tstrength: " + str(result.numPairs))
