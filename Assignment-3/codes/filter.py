from pyspark import SparkContext
data = sc.textFile("/home/jiaul/tcs-big-data/tcs-big-data-datasets/problem-2-data.txt")
pairs = data.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
filteredWords = pairs.filter(lambda x: x[1] > 2)
filteredWords.saveAsTextFile("outputFile")
