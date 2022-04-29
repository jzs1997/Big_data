import sys

from pyspark.sql import SparkSession
# Change permissions for file and folders
# https://www.educative.io/edpresso/how-to-resolve-the-permission-denied-error-in-linux
input_path = 'input.txt'
#print sys info
print("this is the name of the script:", sys.argv[0])
print("number of arguments: ", len(sys.argv))
print("The arguments are:", str(sys.argv))
#create a spark session


#do word count job

#stop session