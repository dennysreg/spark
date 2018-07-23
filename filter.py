from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("playing with python and spark")
sc = SparkContext(conf=conf)

lines = sc.textFile("BC_FORMATEADO.txt")
filteredLines = lines.filter(lambda x: 'AT2' in x)
print(filteredLines.collect())
