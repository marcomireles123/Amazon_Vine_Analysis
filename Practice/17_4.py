# Module 16.4.2 Spark DataFrames and Datasets
# Start Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrameBasics").getOrCreate()
     

dataframe = spark.createDataFrame([
                                   (0,"Here is our DataFrame"),
                                   (1, "We are making one from scratch"),
                                   (2,"This will look very similar to a Panda DataFrame")
], ["id","words"])

dataframe.show()
     

# Read in data from S3 Buckets
from pyspark import SparkFiles
url = "https://s3.amazonaws.com/dataviz-curriculum/day_1/food.csv"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("food.csv"), sep=",", header=True)
     

# Show DataFrame
df.show()
     

# Print our schema
df.printSchema()
     

# sShow the columns
df.columns
     

# Describe our data
df.describe()
     

# Import struct fields that we can use
from pyspark.sql.types import StructField, StringType, IntegerType, StructType
     

# Next we need to create the list of struct fields
schema = [StructField("food", StringType(), True), StructField("price", IntegerType(), True),]
schema
     

# Pass in our fields
final = StructType(fields=schema)
final
     

# Read our data with our new schema
dataframe = spark.read.csv(SparkFiles.get("food.csv"), schema=final, sep=",", header=True)
dataframe.printSchema()
     

dataframe["price"]
     

type(dataframe['price'])
     

dataframe.select('price')
     

type(dataframe.select('price'))
     

dataframe.select('price').show()
     

# Add new column
dataframe.withColumn('newprice', dataframe['price']).show()
# Update column name
dataframe.withColumnRenamed('price','newerprice').show()
# Double the price
dataframe.withColumn('doubleprice',dataframe['price']*2).show()
# Add a dollar to the price
dataframe.withColumn('add_one_dollar',dataframe['price']+1).show()
# Half the price
dataframe.withColumn('half_price',dataframe['price']/2).show()