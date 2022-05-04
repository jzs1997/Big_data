from functools import reduce
import sys
from pyspark.sql import SparkSession
import random
import os
from pyspark import SparkConf

"""
This is a demo for mapPartitions
In this demo, we will compute the minimun, maximum 
and conut(which is always the length of sequence generated)
of a randomly generated sequence consisting of 10000 elements
"""

print("Paht for the file is ", sys.argv[0])
print("Number of cpu is")
print("spark config:")
print(SparkConf().getAll())

#function for mapPartitions
def min_max_count(partition):
    maxNum = 0
    minNum = sys.maxsize
    count = 0
    for element in partition:
        if(element > maxNum):
            maxNum = element
        if(element < minNum):
            minNum = element
        count +=1
    #use [] or all of the results will be flattened into a sequence
    return [(maxNum, minNum, count)]


if __name__ == '__main__':
    #create a long int list
    int_seq = []
    for i in range(10000):
        int_seq.append(random.randint(1, 100000))

    
    spark = SparkSession.builder.appName("mapPartitions").getOrCreate()
    sc = spark.sparkContext
    #set log level
    sc.setLogLevel("Warn")
    print("===========apllicaiton start=========")
    #specify number of partitions
    rddObj = sc.parallelize(int_seq, numSlices=20)
    #show number of partitions
    print("Number of partitions", rddObj.getNumPartitions())
    #use mapPartitions(actually is max min count)
    mapped_rdd = rddObj.mapPartitions(min_max_count)
    #show
    print("Some results for mapped rdd", mapped_rdd.take(10))
    #using reduce to show the final output
    #this will return a tuple rather than an rdd object
    output = mapped_rdd.reduce(lambda a,b:(max(a[0], b[0]), min(a[1], b[1]), a[2] + b[2]))
    #print output
    print("max: {}\n min: {}\n count: {}\n".format(output[0], output[1], output[2]))
    print("============application end============")
    #stop spark session
    spark.stop()
