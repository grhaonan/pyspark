from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName("most_popular_super_hero").getOrCreate()
schema = StructType([StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("./data/marvel_names.txt")

lines = spark.read.text("./data/marvel_graph.txt")
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("value"), "")) - 1).groupBy("id").agg(func.sum("connections").alias("connections"))

most_popular = connections.sort(func.col("connections").desc()).first()

most_popular_name = names.filter(func.col("id") == most_popular[0]).select("name").first()

print(most_popular_name[0] + " is the most popular superhero, with " + str(most_popular[1]) + " co-appearances.")

