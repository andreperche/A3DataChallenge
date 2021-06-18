
#import pyspark as ps
from pyspark.sql import SparkSession


dirSummary = "data/summary"
spark = SparkSession.builder.appName('A3Challenge').getOrCreate()

data = spark.read.format("parquet")\
            .load(dirSummary)\
            .write.format("postgres")\
            .option("host","localhost")\
            .option("partitions", 4) \
            .option("table","rais")\
            .option("user","postgres")\
            .option("database","postgres")\
            .option("schema","public")\
            .load()