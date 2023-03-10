import os
# Find the latest version of spark 3.0  from  http://www.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.3'
spark_version = 'spark-3.3.1'
os.environ['SPARK_VERSION'] = spark_version# Install Spark and Java
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q  http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz
!tar xf $SPARK_VERSION-bin-hadoop3.tgz
!pip install -q findspark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"  # Start a SparkSession

import findspark

findspark.init()

# Start Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Yelp_NLP").getOrCreate()

!wget https://jdbc.postgresql.org/download/postgresql-42.2.17.jar

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CloudETL").config("spark.driver.extraClassPath","/content/postgresql-42.2.17.jar").getOrCreate()

# Read in data from S3 Buckets
from pyspark import SparkFiles
url ="https://marcomireles123-bucket.s3.amazonaws.com/user_data.csv"
spark.sparkContext.addFile(url)
user_data_df = spark.read.csv(SparkFiles.get("user_data.csv"), sep=",", header=True, inferSchema=True)

# Show DataFrame
user_data_df.show()

url ="https://marcomireles123-bucket.s3.amazonaws.com/user_payment.csv"
spark.sparkContext.addFile(url)
user_payment_df = spark.read.csv(SparkFiles.get("user_payment.csv"), sep=",", header=True, inferSchema=True)

# Show DataFrame
user_payment_df.show()

# Join the two DataFrame
joined_df = user_data_df.join(user_payment_df,on="username",how="inner")
joined_df.show()

# Drop null values
dropna_df = joined_df.dropna()
dropna_df.show()

# Load is a sql function to use columns
from pyspark.sql.functions import col

# Filter for only columns with active users
cleaned_df = dropna_df.filter(col("active_user") == True)
cleaned_df.show()

# Create user dataframe to match active_user table
clean_user_df = cleaned_df.select(["id", "first_name","last_name","username"])
cleaned_df.show()

# Create user dataframe to match billing_info table
clean_billing_df = cleaned_df.select(["billing_id","street_address","state","username"])
clean_billing_df.show()

# Create user dataframe to match payment_info table
clean_payment_df = cleaned_df.select(["billing_id","cc_encrypted"])
clean_payment_df.show()