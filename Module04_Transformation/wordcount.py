import sys

from pyspark.sql import SparkSession
# Change permissions for file and folders
# https://www.educative.io/edpresso/how-to-resolve-the-permission-denied-error-in-linux

input_path = sys.argv[1]
print("input path is", input_path)

#print sys info
print("this is the name of the script:", sys.argv[0])
print("number of arguments: ", len(sys.argv))
print("The arguments are:", str(sys.argv))

#do word count job
def wordcount(X):
    return (X, 1)

#stop session

if __name__ == '__main__':
    
    #create a spark session
    spark = SparkSession.builder.appName("wordCount").getOrCreate()

    #read the file:
    output = spark.sparkContext.textFile(input_path)

    #flatmap/ map/ count
    output = output.flatMap(lambda X: X.split(' ')).map(wordcount).reduceByKey(lambda a,b: a + b)

    #print output
    print("=======================Print Start==========================")
    if(output.count() > 20):
        print("As length of rdd is larger than 20 we will only show 20 samples")
        for (key, value) in output.take(20):
            print("{}, {}".format(key, value))
    else:
        for (key, value) in output.collect():
            print("{}, {}".format(key, value))
    print("=======================Print End==========================")
    spark.stop()