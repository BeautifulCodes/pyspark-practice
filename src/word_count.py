
"""Program to count the number of words in a file using pyspark."""


from pyspark.sql import SparkSession
import constants as constants
from operator import add 
from typing import List


class SparkEnv:
	

	def __init__(self):
		self.spark_session = SparkSession\
		.builder\
		.appName("PysparkWordCounter")\
		.getOrCreate()

	def get_spark_session(self):
		return self.spark_session  


class WordCounter:

	""" Utility class, counts the number of words in a file"""

	def txt_filereader(self,spark:SparkSession,path: str) -> str:

		lines = spark.read.text(path).rdd.map(lambda r: r[0])
		return lines


	def word_counter(self,lines):

		count = lines.flatMap(lambda x: x.split(' ')) \
		.map(lambda x: (x,1)) \
		.reduceByKey(add)

		return_value = count.collect()
		return return_value




if __name__ == '__main__':

	obj = WordCounter()
	spark_obj = SparkEnv()

	read_file = obj.txt_filereader(spark_obj.spark_session,constants.WORD_COUNT_FILE_PATH)


	if read_file.count() == 0:
		print("File is empty",file = sys.stderr)
		sys.exit(1)

	else:
		word_counts = obj.word_counter(read_file)

		for key,value in word_counts:
			print("{} - {}".format(key,value))
		













