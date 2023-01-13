# Module 17.4.2 Spark DataFrames and Datasets
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

# 17.4.3 Spark Functions

# Module 17.4.3 Spark Functions
# Start Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrameFunctions").getOrCreate()
     

# Read in data from S3 Buckets
from pyspark import SparkFiles
url ="https://s3.amazonaws.com/dataviz-curriculum/day_1/wine.csv"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("wine.csv"), sep=",", header=True)

# Show DataFrame
df.show()
     

# Order a DataFrame by ascending values
df.orderBy(df["points"].desc())
     

df.orderBy(df["points"].desc()).show(5)
     

#Skill Drill 17.4.3
df.orderBy(df["points"].asc()).show(50)
     

# Import Functions
from pyspark.sql.functions import avg 
df.select(avg("points")).show()
     

# Filter SQL method
df.filter("price<20").show(5)
     

# Filter Python method
df.filter("price<20").show(5)
# Filter by price on certain columns
df.filter("price<20").select(['points','country', 'winery','price']).show(5)

# Filter on exact state
df.filter(df["country"] == "US").show()
     

# Skill Drill 17.4.3 1/2
# Filter Python method
df.filter("price>15").show(5)
# Filter by price on certain columns
df.filter("price>15").select(['points','country', 'winery','price']).show(5)

# Filter on exact state
df.filter(df["province"] == "California").show()
     

# Skill Drill 17.4.3 2/2
# Filter Python method
df.filter("price>15").show(5)
# Filter by price on certain columns
df.filter("price>15").select(['points','country', 'winery','price']).show(5)

# Filter on exact state
df.filter(df["province"] == "California").show()