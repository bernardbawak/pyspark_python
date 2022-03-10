# pyspark_python

 #    NAME: MOVIE RATINGS
 
 #    ABOUT:I used a dataset from movie lens.It contains ratings from movies with a certain period.
      
 #    DELIVERABLES FOR MOVIE RATINGS
 #    movieRatings Folder
 #      ratings-counter.py * file committed through git
 #      u.data * dataset upload in github

        
        
        
        
        
 #     NAME: WORD COUNTER
 
 #     ABOUT: Counting word occurencies from a text file.
 #     
 #     word-counter folder
 #       wordCounter.py * file committed
 #       wordCounterImproveWithRegex.py * file committed
               This improvements takes care of different capitalization, punctuations etc
 #       TheTruthAboutTitanic.txt * text file uploaded on github

 
 
 
 
 #       NAME: YELLOW TAXI
 #      ABOUT: Enforcing schema using spark dataframes
 #      yellowTaxi folder
 #         enforcingSchema.py * file committed
 #         yellow_tripdata_2020.csv (large file)



#QUESTION: This question is related to the yellow taxi(enforcing schema).
 #   In a situation where only one or few column datatype needs to be changed.Do i still have to include every column in the list of structureType? Does spark #provide a way to update or modified an exciting datatype without recreating every columnn in the structype?
    
#schema = StructType([ \
 #       StructField("VendorID", StringType(), True),\
  #      StructField("tpep_pickup_datetime", StringType(), True),\
   #     StructField("tpep_dropoff_datetime", StringType(), True),
    #    StructField("passenger_count", IntegerType(), True),
    #    StructField("trip_distance", DoubleType(), True),
     #   StructField("RatecodeID", StringType(), True),
      #  StructField("store_and_fwd_flag", StringType(), True),
      #  StructField("PULocationID", StringType(), True),
    #    StructField("DOLocationID", IntegerType(), True),
     #   StructField("payment_type", BooleanType(), True),
     #   StructField("fare_amount", IntegerType(), True),
     #  StructField("extra", IntegerType(), True),
     #   StructField("mta_tax", DoubleType(), True),
     #   StructField("tip_amount", IntegerType(), True),
     #   StructField("tolls_amount", IntegerType(), True),
     #   StructField("improvement_surcharge", DoubleType(), True),
     #   StructField("total_amount", IntegerType(), True),
     #   StructField("congestion_surcharge", DoubleType(), True)


    #])
