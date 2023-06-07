#
#Copyright (c) 2021 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark AIRLINES ETL") \
    .getOrCreate()

#The path of our file in S3
input_path ='s3a://goes-se-sandbox01/airlines-csv/'


#This is to deal with tables existing before running this code. Not needed if you're starting fresh.
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

tableName = 'flights'
#Bring data into our Spark job from S3 bucket
flights_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path+tableName+'/'+tableName+".csv")

#Check the schema so we know what we're dealing with
print(f"printing schema")
flights_df.printSchema()

#Sanity check to see how many records we ended up with after Texas filtering
print(f"How many records did we get?")
file_read_cnt = flights_df.count()
print(f"We got: %i " % file_read_cnt)

#Show the final results for one more sanity check
flights_df.show()

print(f"Dedup rows")
flights_df.dropDuplicates().show()

print(f"How many records left?")
file_read_cnt = flights_df.count()
print(f"We got: %i " % file_read_cnt)

print(f"Inserting Data into Hive table \n")

#Write the data into our Hive table
flights_df.\
  write.\
  mode("overwrite").\
  saveAsTable("airlinedata_ws"+'.'+tableName+'_SparkSQL_parquet' , format="parquet")

#Another sanity check to make sure we inserted the right amount of data
print(f"Number of records \n")
spark.sql("Select count(*) as RecordCount from airlinedata_ws."+tableName+'_SparkSQL_parquet').show()

print(f"Retrieve 15 records for validation \n")
spark.sql("Select * from airlinedata_ws."+tableName+"_SparkSQL_parquet limit 15").show()

