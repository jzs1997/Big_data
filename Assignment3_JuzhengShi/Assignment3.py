import sys
from pyspark.sql import SparkSession
#import string

input_path = sys.argv[1]
print("input path is", input_path)

#print sys info
print("this is the name of the script:", sys.argv[0])
print("number of arguments: ", len(sys.argv))
print("The arguments are:", str(sys.argv))

def splitAndFilter(record):
    tokens = record.split(',')
    #filter records with number of tokens less than two
    #drop records due to the exclude
    if((len(tokens)>=2) and (tokens[0] not in list_exclude)):
        #convert numbers from string to float
        tokens = [float(tokens[i]) if i!=0 else tokens[i] for i in range(len(tokens))]
        #convert gene_id into uppercase, find count, minimum, maximum in a singlerecord
        #format for returned object is ('GENEXX', (count, min, max))
        return (tokens[0].upper(), (len(tokens[1:]), min(tokens[1:]), max(tokens[1:])))


def find_count_min_max(a, b):
    #function for reduce by key
    #sum of count, minimum, maximum
    return ((a[0] + b[0]),
    min(a[1], b[1]),
    max(a[2], b[2])
    )


if __name__ == '__main__':
    with open(input_path + 'data/exclude.txt') as file_ecl:
        list_exclude = file_ecl.readlines()
    list_exclude = [gene.replace('\n', '') for gene in list_exclude]
    spark = SparkSession.builder.appName("Assignment3").getOrCreate()
    sc = spark.sparkContext

    sc.setLogLevel("Warn")
    
    rdd = sc.textFile(input_path + 'data/genes.txt')
    #using splitAndFilter will return None if record does not satisfy the requirements
    #so we need to drop Nones
    #last map is used to adjust the format of output
    res = rdd.map(splitAndFilter)\
    .filter(lambda x: x is not None)\
    .reduceByKey(find_count_min_max)\
    .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2]))

    print("length of result rdd is:", res.count())
    print("result:\n", res.collect())

    spark.stop()