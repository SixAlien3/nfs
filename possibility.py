import findspark
findspark.init('/opt/spark-2.1.0-bin-cdh5.9.1/')
from pyspark.sql import SparkSession
import pyspark.rdd

def possibility():
	#sp is a list
	result = []
	for i in range(0, 10000):
		k , v = -1, -1
		for j in range(0, 10000):
			if i == j:
				continue
			else:
				v1 = sp[i].topic_norm.squared_distance(sp[j].topic_norm)
				if v == -1:
					k, v = j, v1
				else:
					if v1 < v:
						k, v = j, v1
		result.append(compare(i, k))
	return sum(result)/10000

def compare(a, b):
	for i in range(0, 20):
		if sp[a].buckets[i] != sp[b].buckets[i]:
			return 0
		if i == 19:
			return 1

if __name__ == '__main__':
	spark = SparkSession.builder.getOrCreate()
	df = spark.read.parquet('/nsf_project/data/bucketed_topics_new.parquet')
	sp = df.rdd.takeSample(False, 10000, 3)

	print(possibility())
	