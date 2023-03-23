# selecting DataFrame columns 

from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()
DF = spark.read.options(header = 'True').csv("titles.csv")
DF.printSchema()

#selecting data using column names
DF2 = DF.select(DF.id, DF.title, DF.type, DF.description).show()
