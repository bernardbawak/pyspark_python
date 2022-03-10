import re
from pyspark import SparkConf, SparkContext
import collections
#This solution removes different variants of words that results from capitalization,punctuations etc

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())



sc = SparkContext("local[3]", "word count")
sc.setLogLevel("ERROR")

input= sc.textFile("word-counter/TheTruthAboutTitanic.txt")

words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
        print("{} : {}".format(word, count))
        #print (word, count)


#Notes: I will try to replicate the solution without using packages or library.
# By writing a function that converts the words to lowerCase and removes symbols and punctuations