from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()

DF = spark.read.options(header = 'True').csv("titles.csv")
DF.printSchema()

# printing contents of a DataFrame this is the default print 20 lines, and 20 characters in a column
DF.show()

# Printing custom number of columns. Printing 3 rows and displaying all comlumn contents in full
DF.show(3, truncate = False)

# printing 10 rows, 10 characters and vertically printing rows and columns
DF.show(10, truncate = 10, vertical = True)
