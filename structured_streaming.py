from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract


spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

access_lines = spark.readStream.text("./data/logs")

content_size_exp = r'\s(\d+)$'
status_exp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logs_df = access_lines.select(regexp_extract('value', hostExp, 1).alias('host'),
                                regexp_extract('value', timeExp, 1).alias('timestamp'),
                                regexp_extract('value', generalExp, 1).alias('method'),
                                regexp_extract('value', generalExp, 2).alias('endpoint'),
                                regexp_extract('value', generalExp, 3).alias('protocol'),
                                regexp_extract('value', status_exp, 1).cast('integer').alias('status'),
                                regexp_extract('value', content_size_exp, 1).cast('integer').alias('content_size'))


status_counts_df = logs_df.groupBy('status').count()

query = (status_counts_df.writeStream.outputMode('complete').format('console').queryName('counts').start())

query.awaitTermination()

spark.stop()

