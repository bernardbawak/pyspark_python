from cgitb import reset
from itertools import count
from pyspark import SparkConf, SparkContext
import collections


#Countings words greater that two characters from a text file

sc = SparkContext("local[3]", "word count")
sc.setLogLevel("ERROR")

lines= sc.textFile("word-counter/TheTruthAboutTitanic.txt")

words = lines.flatMap(lambda line: line.split(" ")) 


wordCounts = words.countByValue()
for word, count in wordCounts.items():
        print("{} : {}".format(word, count))     
        #print (word, count)

