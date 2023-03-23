from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("ReadingCSV").getOrCreate()

DF = spark.read.json("qa_Cell_Phones_and_Accessories.json")
DF.printSchema()
DF.show()
