from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("min_temperature").getOrCreate()

schema = StructType([StructField("station_id", StringType(), True), \
                        StructField("date", IntegerType(), True), \
                        StructField("measure_type", StringType(), True), \
                        StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("./data/1800.csv")
df.printSchema()

min_temps = df.filter(df.measure_type == "TMIN")

station_temps = min_temps.select("station_id", "temperature")
station_temps.show()

#  use the alias() method in conjunction with agg() (which allows you to provide an alias for an aggregated column directly).
min_temps_by_station = station_temps.groupBy("station_id").agg(min("temperature").alias("min_temperature"))
min_temps_by_station.show()

min_temps_by_station_f = min_temps_by_station.withColumn("temperature", func.round(func.col("min_temperature") * 0.1 * (9.0 / 5.0) + 32.0, 2)).select("station_id", "temperature").sort("temperature")

results = min_temps_by_station_f.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()

