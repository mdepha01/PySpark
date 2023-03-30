# reviewing drop() function
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName("DropFunction").getOrCreate()

# creating dataframe from a csv file
df1 = spark.read.options(header = 'True').csv("titles.csv")
df1.printSchema()

# droping a singel column , imdb_votes
df2 = df1.drop("imdb_votes")
df2.printSchema()

# droping multiple columns 
df3 = df1.drop("imdb_score", "imdb_votes", "imdb_id")
df3.printSchema()
