
#Importing minimum packages for spark.Sparkcontext is the fundamental framework we will need to create rdd
#We cant create a sparkcontext without sparkconf which allows us to configure the sparkcontext to decide if it should be run on some computer or some other cluster
#collection will help in sorting


from pyspark import SparkConf, SparkContext
import collections


#setting up our spark configuration to run locally ,not on a cluster.This is splitted amoung multiple cpu cores but it can be set o=to run locally in a single process and  thread"local"
sc = SparkContext("local[3]", "ratingsHistogram")


#sc.setLogLevel("ERROR")

#loading our movie rating data file.Creating our rdd 
#Sketchy representation of our file.The first element represents the userID,2nd element -the movieid , third element -rating value and 4th element-timestamp.
#196 243 3 34322333
#22  343 2 43233244

lines = sc.textFile("movieRatings/u.data")
#split base on white space individual fields into a list of elements.We are interestes in the third element on index2 that represents stars and assign it ti a new variable call "ratings"
ratings = lines.map(lambda x: x.split()[2])
#Using an action Variable "CountByValue". 
result = ratings.countByValue() #Our result object will look like a turple(ratings, sum of that particular ratings)

#sorting the result using collections and sort method on the ratings 
sortedResults = collections.OrderedDict(sorted(result.items()))
print(" P R I N T I N G   S U M  O F  E A C H  R A T I N G")
for key, value in sortedResults.items():
    #print "%s %i" % (key, value)
    #print("{} : {}".format(key, value))
    print (key, value)



#My next task : I will be creating a column header.I will add more columns and apply some transformations