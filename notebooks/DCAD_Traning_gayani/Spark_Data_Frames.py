# Databricks notebook source
import pandas as pd
from pyspark.sql import Row
from datetime import datetime, date

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
print(df)
df.show()
display(df)

# COMMAND ----------

df.printSchema

# COMMAND ----------

#Create a PySpark DataFrame with an explicit schema.
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
display(df)

# COMMAND ----------

#df.show(1)
df.head(1)

# COMMAND ----------

#reading data files in pyspark
customerdf = spark.read.json("dbfs:/FileStore/tables/dcad_data/customer.json")
customerdf.printSchema() #print the schema
customerdf.show()

# COMMAND ----------

#creting or replacing tempory view and use sql queries
customerdf.createOrReplaceTempView("customer")
customerdf = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer")
customerdf.show()

# COMMAND ----------

#writing the dataframe in to a parquet files
customerdf.write.parquet("dbfs:/FileStore/tables/dcad_data/customerdata_new_parquet.parquet")


# COMMAND ----------

#reading the parket file in to a dataframe
parquetFileDf = spark.read.parquet("dbfs:/FileStore/tables/dcad_data/customerdata_new_parquet.parquet")
#creating or replacing a view of the parquet file and laod the data to a new dataframe
parquetFileDf.createOrReplaceTempView("customer_parquetFile")
male_urtomerParquet = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer_parquetFile  WHERE gender= 'M' ")
male_urtomerParquet.show()

# COMMAND ----------

female_urtomerParquet = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer_parquetFile  WHERE gender= 'F' ")
female_urtomerParquet.show()

# COMMAND ----------

#Schema merging in Parquet files
curtomerParquetDf = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer_parquetFile ")
curtomerParquetDf.show()

#partition by gender 
curtomerParquetDf.write.partitionBy("gender").mode("overwrite").parquet("dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet")


# COMMAND ----------

#Retrieving from a partitioned Parquet file
parDFMale=spark.read.parquet("dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet/gender=M")
parDFMale.show(truncate=False)

# COMMAND ----------

parDF2Female=spark.read.parquet("dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet/gender=F")
parDF2Female.show(truncate=False)

# COMMAND ----------

#Creating a table on Partitioned Parquet file

spark.sql("CREATE TEMPORARY VIEW FEMALE_PERSON USING parquet OPTIONS (path \"dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet/gender=F\")")



# COMMAND ----------

#Querying using the parttioned parquetfile view
spark.sql("SELECT * FROM FEMALE_PERSON" ).show()
parDF2male=spark.read.parquet("dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet/gender=M")
parDF2male.show(truncate=False)

# COMMAND ----------


spark.sql("CREATE TEMPORARY VIEW MALE_PERSON USING parquet OPTIONS (path \"dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet/gender=M\")")
spark.sql("SELECT * FROM MALE_PERSON" ).show()


# COMMAND ----------

#merging the two tables

# Read the partitioned files in to a one dataframe and merge them 
mergedDF = spark.read.option("mergeSchema", "true").parquet("dbfs:/FileStore/tables/dcad_data/customers_by_gender.parquet")
mergedDF.printSchema()
mergedDF.display()
#use the merged data frame to query by gender 
mergedDF.createOrReplaceTempView("customer_mergedDF")
mergedDF_Female = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer_mergedDF where gender='F' ")
mergedDF_Female.show()

mergedDF_male = spark.sql("SELECT firstname, lastname,email_address,gender FROM customer_mergedDF where gender='M' ")
mergedDF_male.show()

# COMMAND ----------

#aggregate data in a dataframe 

mergedDF.groupBy("gender").count().show(truncate=False)

# COMMAND ----------

#define a struct type object
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

#defining nested strcut type in pyspark
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.display()

# COMMAND ----------

from pyspark.sql.functions import col,struct,when
#Adding and updating a new struct type named otherInfo with a new column Salary_Grade 
#this explins how to cop columns from one struct to other and add and update new struct
updatedDF = df2.withColumn("OtherInfo", 
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)

# COMMAND ----------

#Using SQL ArrayType and MapType
#Array type = all the values inside the array types have the same data type which is defined when the array type is defined
arrayStructureSchema = StructType([
    StructField('name', StructType([
       StructField('firstname', StringType(), True),
       StructField('middlename', StringType(), True),
       StructField('lastname', StringType(), True)
       ])),
       StructField('hobbies', ArrayType(StringType()), True),
       StructField('properties', MapType(StringType(),StringType()), True)
    ])



# COMMAND ----------

#will provide the schema of the strcut type and we can use this to geerate the schema by storing this inside a file
df2.schema.json()
#df2.schema.simpleString() #this will list down relatively a simple schema format

# COMMAND ----------

import json

#loading the same schema from df2 dataframe to a new dataframe
schemaFromJson = StructType.fromJson(json.loads(df2.schema.json()))
df3 = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData),schemaFromJson)
df3.printSchema()

# COMMAND ----------


# Creating StructType object struct from DDL String
ddlSchemaStr = "'fullName' STRUCT<'first': STRING, 'last: STRING, 'middle': STRING>,'age' INT, 'gender' STRING"
ddlSchema = StructType.fromDDL(ddlSchemaStr)
ddlSchema.printTreeString()

# COMMAND ----------

#print(df.schema.fieldNames.contains("firstname"))
#print(df.schema.contains(StructField("firstname",StringType,true)))
print(df3.schema.simpleString().find("firstname:"))
print("name" in df3.schema.fieldNames())
print(StructField("gender",StringType(),True) in df3.schema)

# COMMAND ----------

#find the filed in the schema

from pyspark.sql import Row
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
df.printSchema()

print(df.columns)
#['name', 'prop']
print("name" in df.columns)
# True

#case in-sensitive
print("name".upper() in (name.upper() for name in df.columns))
# True

#to check if you have nested columns
print(df.schema.simpleString())
#struct<name:string,prop:struct<hair:string,eye:string>>

print(df.schema.simpleString().find('hair:'))
#31

print('hair:' in df.schema.simpleString())
#True

from pyspark.sql.types import StructField,StringType

print("name" in df.schema.fieldNames())
print(StructField("name",StringType(),True) in df.schema)


# COMMAND ----------

#Create PySpark ArrayType
from pyspark.sql.types import StringType, ArrayType, StructType,StructField
from pyspark.sql.functions import explode

arrayCol = ArrayType(StringType(),False)

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()


#PySpark ArrayType (Array) Functions
#explode() -Use explode() function to create a new row for each element in the given array column.

df.select(df.name, explode(df.languagesAtSchool)).show()

#Split() : sql function returns an array type after splitting the string column by delimiter

from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

#array() Use array() function to create a new array column by merging the data from multiple columns. All input columns must have the same data type. The below example combines the data from currentState and previousState and creates a new column states.

from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

#array_contains():array_contains() sql function is used to check if array column contains a value. Returns null if the array is null, true if the array contains the value, and false otherwise.
from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()

# COMMAND ----------

#PySpark Check if Column Exists in DataFrame --name is the column name here
listColumns=df.columns
"name"  in listColumns 

#2. Check by Case insensitive
print("name".upper() in (name.upper() for name in df.columns))

#3. Check if Column exists in Nested Struct DataFrame
df.schema.simpleString().find("name:")
#or
"name:" in df.schema.simpleString()

#4. Check if a Field Exists in a DataFrame
from pyspark.sql.types import StructField,StringType

print("name" in df.schema.fieldNames())
print(StructField("name",StringType(),True) in df.schema)

# COMMAND ----------

from pyspark.sql import Column
from pyspark.sql.functions import upper
#accessing a column in a datarame
df.name
df.select(df.name).show()
#Assign new Column instance.
df.withColumn('upper_name', upper(df.name)).show()

#To select a subset of rows, use DataFrame.filter().
df.filter(df.name == 'James,,Smith').show()

# COMMAND ----------

#applying a function
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('string') #this is based on our return type and 
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + '1'

df.select(pandas_plus_one(df.name).alias("modified Name")).show()

# COMMAND ----------

#another function to map in pandas
def pandas_filter_func(iterator):
    for pandas_df in iterator:
        yield pandas_df[pandas_df.name == 'James,,Smith']
        
df.mapInPandas(pandas_filter_func, schema=df.schema).show()
df.groupby('currentState').count().alias('state_Count').show()

# COMMAND ----------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()

# COMMAND ----------

#aliasing columns in pyspark 
simpleData = [("James","Sales","NY",90000,34,10000),
  ("Michael","Sales","NV",86000,56,20000),
  ("Robert","Sales","CA",81000,30,23000),
  ("Maria","Finance","CA",90000,24,23000),
  ("Raman","Finance","DE",99000,40,24000),
  ("Scott","Finance","NY",83000,36,19000),
  ("Jen","Finance","NY",79000,53,15000),
  ("Jeff","Marketing","NV",80000,25,18000),
  ("Kumar","Marketing","NJ",91000,50,21000)
]
schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

df.groupBy("state").sum("salary").show()

#I would like to rename sum(salary) to sum_salary
#so in this case use sum() aggregate function in SQL function instead
from pyspark.sql.functions import sum
df.groupBy("state") \
  .agg(sum("salary").alias("sum_salary")).show()

#Use withColumnRenamed() to Rename groupBy()
df.groupBy("state","department") \
  .sum("salary") \
  .withColumnRenamed("sum(salary)", "sum_salary") \
  .select("State" , "department" , "sum_salary") \
  .show()

#Use select() Transformation
from pyspark.sql.functions import col
df.groupBy("state") \
  .sum("salary") \
  .select(col("state"),col("sum(salary)").alias("sum_salary_Sql_col_functions")) \
  .show()

#Use SQL Expression for groupBy()
df.createOrReplaceTempView("EMP")
spark.sql("select state, sum(salary) as sum_salary from EMP group by state").show()