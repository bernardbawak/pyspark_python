from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType,StructField,StructType, BooleanType, ArrayType, IntegerType
import pyspark.sql.dataframe
import pyspark.sql.functions as F



spark = SparkSession.builder.appName("enforceSchema").getOrCreate()
#print (spark)  #Confirmed spark was successfully started

df_pyspark=spark.read.option("header","true").option("inferSchema","true").csv("yellowTaxi/yellow_tripdata_2020-01.csv")


print("D E F A U A L T    S C H E C H E M A")
df_pyspark.printSchema()

#df_pyspark.show() #dISPLAYING THE CSV FILE


#Displaying column vendorID and data
df_pyspark.select('VendorID').show()

#Display only column name and datatype
#returns df_pyspark.select('VendorID')






#Manipulating and enforcing a new schema 

print(" S C H E M A   E N F O R C E D     W I T H   S T R U C T Y P E")

schema = StructType([ \
        StructField("VendorID", StringType(), True),\
        StructField("tpep_pickup_datetime", StringType(), True),\
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", StringType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", StringType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", BooleanType(), True),
        StructField("fare_amount", IntegerType(), True),
        StructField("extra", IntegerType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", IntegerType(), True),
        StructField("tolls_amount", IntegerType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", IntegerType(), True),
        StructField("congestion_surcharge", DoubleType(), True)


    ])
df_new = spark.read.option("header","true").schema(schema).csv('yellowTaxi/yellow_tripdata_2020-01.csv')
df_new.printSchema()