from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName("most_popular_super_hero").getOrCreate()
schema = StructType([StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("./data/marvel_names.txt")

lines = spark.read.text("./data/marvel_graph.txt")
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), "")) - 1).groupBy("id").agg(func.sum("connections").alias("connections"))

#  This will fetch the minimum value
least_popular_count = connections.agg(func.min("connections")).first()[0]


least_popular_connections = connections.filter(func.col("connections") == least_popular_count)

least_popular_connections_names = least_popular_connections.join(names, "id")

print("The following characters have only " + str(least_popular_connections) + " connection(s):")

