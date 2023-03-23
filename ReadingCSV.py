# creating a spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()

# reading csv file ("option ensures that the first row of the csv is the header"), printing DataFrame Schema and showing all contents of DataFrame
MovieTitleDF = spark.read.option("header", True).csv("titles.csv")
MovieTitleDF.printSchema()
MovieTitleDF.show()
