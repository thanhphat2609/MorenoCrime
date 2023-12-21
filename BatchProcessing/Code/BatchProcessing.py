
# ======================================================================== STEP 1: CONFIG CONNECT TO HIVE FOR USING HIVE WITH PYSPARK ========================================================================

from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit, col, when, row_number
from pyspark.sql.window import Window

# Connect to Hive	
spark = SparkSession.builder.appName("BatchProcessing").config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse").enableHiveSupport().getOrCreate()


# ======================================================================== STEP 2: READ AND PREPROCESSING DATASET ========================================================================

# Function read data
def read_dataset_full(input_path):
	df = spark.read.csv(input_path, sep=" ", header=False, inferSchema=True)
	return df
def read_dataset_connect(input_path):
	df = spark.read.csv(input_path, sep=",", header=False, inferSchema=True)
	return df


# Function Preprocessing
  # Change column name
def change_column_name(dataframe, column_1):
	df = dataframe.withColumnRenamed('_c0', column_1)
	return df

def change_column_name_2(dataframe, column_1, column_2):
	df_temp = dataframe.withColumnRenamed('_c0', column_1)
	df = df_temp.withColumnRenamed('_c1', column_2)
	return df


  # Mapping data
def mapping_data(df_morenocrime, df_name, df_sex):
	# Cast FromUser to Int
	df_morenocrime = df_morenocrime.withColumn("FromUser", df_morenocrime["FromUser"].cast("int"))

	# Join DataFrame df_morenocrime with df_name for information FromUser
	df_morenocrime = df_morenocrime.join(df_name, df_morenocrime["FromUser"] == df_name["Id"], "left_outer").drop("Id")
	
	# Join DataFrame df_morenocrime with df_sex for information FromUser
	df_morenocrime = df_morenocrime.join(df_sex, df_morenocrime["FromUser"] == df_sex["Id"], "left_outer").drop("Id")
	
	return df_morenocrime  
     
  # Create dataframe df_morenocrime with join
def join_dataframe(df_name, df_sex, df_role, df_connect):
	# Make an identiy
	w = Window().orderBy(lit('A'))
	
	# Create an identity for df_name, df_sex
	df_name = df_name.withColumn('Id', row_number().over(w))
	df_sex = df_sex.withColumn('Id', row_number().over(w))
	df_role = df_role.withColumn('Id', row_number().over(w))
	df_connect = df_connect.withColumn('Id', row_number().over(w))
	
	# Join dataframe on column id
	df_morenocrime = df_role.join(df_connect, on = ["Id"], how = 'inner')
	
	# Matching data for df_morenocrime
	df_morenocrime = mapping_data(df_morenocrime, df_name, df_sex)
	
	# Create an identiy for df_morenocrime
	df_morenocrime = df_morenocrime.withColumn('Id', row_number().over(w))
	
	# Join dataframe on column id for df_name_sex
	df_name_sex = df_name.join(df_sex, on = ['Id'], how = 'inner').drop("Id")
	
	return df_morenocrime, df_name_sex

  # Slice dataframe
def slice_df_new(df,start,end):
	df_new = spark.createDataFrame(df.limit(end).tail(end - start))
	return df_new

# ==================================================================================== USE FUNCTION ====================================================================================
# Path for all data
input_path_name = 'hdfs://localhost:9000/user/thanhphat/datalake/moreno_crime/ent.moreno_crime_crime.person.name'
input_path_sex = 'hdfs://localhost:9000/user/thanhphat/datalake/moreno_crime/ent.moreno_crime_crime.person.sex'
input_path_role = 'hdfs://localhost:9000/user/thanhphat/datalake/moreno_crime/rel.moreno_crime_crime.person.role'
input_path_connect = 'hdfs://localhost:9000/user/thanhphat/datalake/moreno_crime/out.moreno_crime_crime'


# Read data
df_name = read_dataset_full(input_path_name)
df_sex = read_dataset_full(input_path_sex)
df_role = read_dataset_full(input_path_role)
df_connect = read_dataset_connect(input_path_connect)


# Create column name
column_name = "PersonName"
column_sex = "PersonSex"
column_role = "PersonRole"
column_from = "FromUser"
column_to = "ToCrime"


# Change column name
df_name = change_column_name(df_name, column_name)
df_sex = change_column_name(df_sex, column_sex)
df_role = change_column_name(df_role, column_role)
df_connect = change_column_name_2(df_connect, column_from, column_to)


# Create dataframe df_morenocrime, df_name_sex
df_morenocrime, df_name_sex = join_dataframe(df_name, df_sex, df_role, df_connect)


# ================================================================== STEP 3: EXPORT DATA FILE FOR ENRICH, STREAM =====================================================================

# Export data for Enrich
df_morenocrime.coalesce(1).write.mode("overwrite").csv("hdfs://localhost:9000/user/thanhphat/EnrichData/", header = True)


# Export data for Stream data 
df_stream = slice_df_new(df_morenocrime, 1000, 1476)
df_stream.coalesce(1).write.mode("overwrite").csv("/home/thanhphat/Downloads/StreamData/", header = True)


# ======================================================================== STEP 4: CREATE TABLE WITH DATAFRAME ========================================================================

# Function create database
def create_database(database_name):
	spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
	spark.sql(f"USE {database_name}")

def create_tabledata(dataframe, table_name):
	dataframe.write.mode('overwrite').saveAsTable(f"{table_name}")
	
# Create database
database_name = 'Moreno_Crime'
create_database(database_name)

# Create table with dataframe	
table_name = 'Dim_PersonDetail'
create_tabledata(df_name_sex, table_name)
table_name = 'Dim_PersonRole'
create_tabledata(df_role, table_name)

# Create batch table
table_name = 'Fact_MorenoCrime'
df_batch = slice_df_new(df_morenocrime, 0, 1000)
create_tabledata(df_batch, table_name)
