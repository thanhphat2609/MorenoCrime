# ======================================================================== STEP 1: CONFIG SPARK READ DATA STREAM ========================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, get_json_object, schema_of_json
from pyspark.sql.types import StructType, StringType, StructField, LongType, IntegerType
from ast import literal_eval
import pandas

# Connect to Hive
spark = SparkSession.builder.appName("ReadDataStream") \
	.config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
	.config("hive.exec.dynamic.partition", "true") \
	.config("hive.exec.dynamic.partition.mode", "nonstrict") \
	.enableHiveSupport().getOrCreate()

# ======================================================================== STEP 2: READ STREAM DATA ========================================================================

def read_stream_data():
	# Read data from Kafka
	stream_data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "streamTopic").option("startingOffsets", "latest").load()

	# Choose column value and cast to string
	stream_data = stream_data.selectExpr("CAST(value AS STRING)")

	# Define schema for data
	schema = StructType([
		StructField("PersonRole", StringType(), True),
		StructField("FromUser", LongType(), True),
		StructField("ToCrime", LongType(), True),
		StructField("PersonName", StringType(), True),
		StructField("PersonSex", LongType(), True),
		StructField("Id", LongType(), True)
	])

	# Parse JSON data and apply schema
	df_morenocrime = stream_data.select(from_json(col("value"), schema).alias("MorenoCrime")).select("MorenoCrime.*")
	
	return df_morenocrime

# Show data

df_morenocrime = read_stream_data()

# ======================================================================== STEP 3: INSRT DATA INTO HIVE ========================================================================
def write_streamdata_hive(dataframe):
	# Config write stream data to Hive
	dataframe = dataframe.writeStream.format("parquet") \
		    	.outputMode("append") \
			.option("checkpointLocation", "hdfs://localhost:9000/user/thanhphat/checkpoint") \
			.option("path","hdfs://localhost:9000/user/hive/warehouse/moreno_crime.db/fact_morenocrime") \
			.option("database", "moreno_crime") \
   			.option("table", "fact_morenocrime") \
			.start()
	return dataframe

df_morenocrime = write_streamdata_hive(df_morenocrime)

df_morenocrime.awaitTermination()

