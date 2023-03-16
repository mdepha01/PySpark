# importing pyspark
from pyspark import SparkContext, SparkConf

#configuring pyspark running environment, app name, and spark context
Conf = SparkConf().setMaster("local").setAppName("SparkBasics")
sc = SparkContext.getOrCreate(conf = Conf)
