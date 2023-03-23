from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()

MovieTitleDF = spark.read.options(header = 'True', inferSchema = 'True').csv("titles.csv")

#writing DataFrame to CSV after performing transformations

MovieTitleDF.write.options(header ='True', delimiter = ',').csv("FromDataFrame/table")

#------------------------saving options-------------------------------------------------------
#PySpark DataFrameWriter also has a method mode() to specify saving mode.
#overwrite – mode is used to overwrite the existing file.
#append – To add the data to the existing file.
#ignore – Ignores write operation when the file already exists.
#error – This is a default option when the file already exists, it returns an error.

