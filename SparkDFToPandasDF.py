# converting spark DataFrame to PandasDataFrame
# spark and pandas configuration
from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()

#creating dataframe DF from csv file
DF = spark.read.options(header = 'True').csv("titles.csv")
DF.printSchema()
DF.show()

#converting spark DataFrame to PandasDataFrame and printing the results
PandasDF = DF.toPandas()
print(PandasDF)
