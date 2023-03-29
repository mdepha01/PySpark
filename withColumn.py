from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName("withColumn").getOrCreate()

# creating a DataFrame from reading a csv file
df1 = spark.read.options(header = 'True').csv("titles.csv")
df1.printSchema()

# changing column datatype. release_year column from string to int

df2 = df1.withColumn("release_year", df1.release_year.cast('integer'))
df2.printSchema()

# creating a new column from existing column. the colum is added at the end of the table.
df3 = df2.withColumn("release_year2", df2.release_year)
df3.printSchema()

# renaming a column. renaming release_year2 to new_release year
df4 = df3.withColumnRenamed("release_year2", "new_release_year")
df4.printSchema()

# droping a column 
df5 = df4.drop("new_release_year").printSchema()
