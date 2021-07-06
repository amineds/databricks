# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Quickstart for Data Engineers
# MAGIC Welcome to the quickstart lab for data analysts on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Transform and store your data in a reliable and performance Delta Lake
# MAGIC 3. Use Update,Delete,Merge,Schema Evolution and Time Travel Capabilities of Delta Lake
# MAGIC 
# MAGIC ## The Use Case
# MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing Your Enterprise Data Lake
# MAGIC Databricks enables an architecture where your analytics is decoupled from your data storage. This allows organizations to store their data cost effectively in Azure Storage and share their data across best of breed tools in Azure without duplicating it in data silos. 
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/Delta%20Lakehouse.png" width=800>
# MAGIC 
# MAGIC In this notebook, we focus exclusively on the **Data Engineers** user. Subsequent quickstart labs will demonstrate data science, SQL Analytics and machine learning on the same data set without duplicating it. 
# MAGIC 
# MAGIC Run the code below to set up your storage access in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing Azure Storage in Databricks
# MAGIC There are two common ways to access Data Lake stores in Azure Databricks: More information [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json). 
# MAGIC 1. Mounting your storage container to the Databricks workspace to be shared by all users and clusters. This can be done via a Service principal or using Access Keys
# MAGIC 2. Passing your Azure AD credentials to the storage for fine-grained access security

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Azure Storage using an Access Key or Service Principal
# MAGIC We will mount an Azure blob storage container to the workspace using a shared Access Key. More instructions can be found [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage#--mount-azure-blob-storage-containers-to-dbfs). 

# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://channelsa-datasets@channelsapublicprodblob.blob.core.windows.net/ADB-Quickstart/KKBox-Dataset",
  mount_point = "/mnt/adbquickstart-source",
  extra_configs = {
    f"fs.azure.account.key.channelsapublicprodblob.blob.core.windows.net":"xxxxxxxxxx" #to provide during workshop
  }
)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/adbquickstart")

# COMMAND ----------

BLOB_CONTAINER = "blobcontainer"
BLOB_ACCOUNT = "blobstor358476"
ACCOUNT_KEY = "xxxxxxxxxx" #to provide during workshop
ADLS_CONTAINER = "adlscontainer"
ADLS_ACCOUNT = "adls358476"

# COMMAND ----------

DIRECTORY = "/"
MOUNT_PATH = "/mnt/adbquickstart-target"

dbutils.fs.mount(
  source = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT}.blob.core.windows.net/",
  mount_point = MOUNT_PATH,
  extra_configs = {
    f"fs.azure.account.key.{BLOB_ACCOUNT}.blob.core.windows.net":ACCOUNT_KEY
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC Once mounted, we can view and navigate the contents of our container using Databricks `%fs` file system commands.

# COMMAND ----------

# MAGIC %fs ls /mnt/adbquickstart-source/

# COMMAND ----------

# MAGIC %fs head /mnt/adbquickstart-source/members/members_v3.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Your Data
# MAGIC In 2018, [KKBox](https://www.kkbox.com/) - a popular music streaming service based in Taiwan - released a [dataset](https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data) consisting of a little over two years of (anonymized) customer transaction and activity data with the goal of challenging the Data & AI community to predict which customers would churn in a future period.  
# MAGIC 
# MAGIC The primary data files are organized in the storage container:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_filedownloads.png' width=150>
# MAGIC 
# MAGIC Read into dataframes, these files form the following data model:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_schema.png' width=150>
# MAGIC 
# MAGIC Each subscriber is uniquely identified by a value in the `msno` field of the `members` table. Data in the `transactions` and `user_logs` tables provide a record of subscription management and streaming activities, respectively.  

# COMMAND ----------

import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.stat import Statistics
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql import functions
from pyspark.sql.functions import isnan, when, count, col
import pandas as pd
import numpy as np
import matplotlib.pyplot as mplt
import matplotlib.ticker as mtick

# COMMAND ----------

import shutil
from pyspark.sql.types import *
# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS kkbox CASCADE')

# drop any old delta lake files that might have been created
shutil.rmtree('/dbfs/mnt/adbquickstart-target/bronze', ignore_errors=True)
shutil.rmtree('/dbfs/mnt/adbquickstart-target/gold', ignore_errors=True)
shutil.rmtree('/dbfs/mnt/adbquickstart-target/silver', ignore_errors=True)
shutil.rmtree('/dbfs/mnt/adbquickstart-target/checkpoint', ignore_errors=True)
# create database to house SQL tables
_ = spark.sql('CREATE DATABASE kkbox')

# COMMAND ----------

# MAGIC %md
# MAGIC ##In this Demo notebook we will showcase some of the most common scenarios Data Engineers encouter while working on ingesting and processing data
# MAGIC ####1. Ingest Data in Batch Process
# MAGIC ####2. Ingest Data from a streaming source
# MAGIC ####3. Perform operations such as Update, Merge , Delete on data

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 1  : INGEST DATA in BATCH PROCESS (Reading CSV or Parquet File)
# MAGIC  ##### In this scenario we will ingest an inital load of transactional data to Delta format. We will ingest two data sets : (Transaction Dataset : Parquet Format) and (Members data : csv Format) and convert it to Delta(bronze layer)

# COMMAND ----------

# DBTITLE 1,Prep Transactions Dataset - Parquet Files to Delta
# transaction dataset schema
transaction_schema = StructType([
  StructField('msno', StringType()),
  StructField('payment_method_id', IntegerType()),
  StructField('payment_plan_days', IntegerType()),
  StructField('plan_list_price', IntegerType()),
  StructField('actual_amount_paid', IntegerType()),
  StructField('is_auto_renew', IntegerType()),
  StructField('transaction_date', DateType()),
  StructField('membership_expire_date', DateType()),
  StructField('is_cancel', IntegerType())  
  ])

# read data from parquet
transactions = (
  spark
    .read
    .parquet(
      '/mnt/adbquickstart-source/transactions_pq',
      schema=transaction_schema,
      header=True,
      dateFormat='yyyyMMdd'
      )
    )

# persist in delta lake format
( transactions
    .write
    .format('delta')
    .partitionBy('transaction_date')
    .mode('overwrite')
    .save('/mnt/adbquickstart-target/bronze/transactions')
  )

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE kkbox.transactions
  USING DELTA 
  LOCATION '/mnt/adbquickstart-target/bronze/transactions'
  ''')

# COMMAND ----------

display(transactions)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kkbox.transactions

# COMMAND ----------

# DBTITLE 1,Prep Members Dataset - CSV to Delta Ingest
# members dataset schema
member_schema = StructType([
  StructField('msno', StringType()),
  StructField('city', IntegerType()),
  StructField('bd', IntegerType()),
  StructField('gender', StringType()),
  StructField('registered_via', IntegerType()),
  StructField('registration_init_time', DateType())
  ])

# read data from csv
members = (
  spark
    .read
    .csv(
      'dbfs:/mnt/adbquickstart-source/members/members_v3.csv',
      schema=member_schema,
      header=True,
      dateFormat='yyyyMMdd'
      )
    )

# persist in delta lake format
(
  members
    .write
    .format('delta')
    .mode('overwrite')
    .save('/mnt/adbquickstart-target/bronze/members')
  )

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE kkbox.members 
  USING DELTA 
  LOCATION '/mnt/adbquickstart-target/bronze/members'
  ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kkbox.members

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 2  : INGEST DATA from a streaming source
# MAGIC  ##### For demo purpose we create a stream from the user_log file which is csv format and then convert to Delta which acts as a sink. In real time we would be using Eventhub or Kafka

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val log_schema = StructType(Seq(
# MAGIC   StructField("msno", StringType, true), 
# MAGIC   StructField("date", StringType, true),
# MAGIC   StructField("num_25", IntegerType , true),  
# MAGIC   StructField("num_50", IntegerType, true),
# MAGIC   StructField("num_75", IntegerType, true),
# MAGIC   StructField("num_985", IntegerType, true),
# MAGIC   StructField("num_100", IntegerType, true),
# MAGIC   StructField("num_unq", IntegerType, true),
# MAGIC   StructField("total_secs", StringType, true),
# MAGIC ))
# MAGIC 
# MAGIC val streamingDF = spark.readStream.format("com.databricks.spark.csv").schema(log_schema).option("mode","APPEND").load("dbfs:/mnt/adbquickstart-source/user_logs/")

# COMMAND ----------

# DBTITLE 1,Ingest Streaming data to Delta
# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC 
# MAGIC streamingDF 
# MAGIC     .repartition(1) 
# MAGIC     .writeStream 
# MAGIC     .format("delta")
# MAGIC     .outputMode("append")
# MAGIC     .option("checkpointLocation", "/mnt/adbquickstart-target/checkpoint") 
# MAGIC     .start("dbfs:/mnt/adbquickstart-target/bronze/user_logs/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adbquickstart-target/bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE TABLE kkbox.user_logs 
# MAGIC   USING DELTA 
# MAGIC   LOCATION 'dbfs:/mnt/adbquickstart-target/bronze/user_logs/'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Now we have ingested the raw data (both Batch and streaming) and landed it in Delta format. The Next Step is to do some ETL and move it to Silver layer
# MAGIC <img src="https://kpistoropen.blob.core.windows.net/collateral/quickstart/etl.png" width=1500>
# MAGIC 
# MAGIC #####For this Demo we are moving the just data as is from Bronze to Silver landing Zone

# COMMAND ----------

## Read the Bronze Data
transactions_bronze = spark.read.format("delta").load('/mnt/adbquickstart-target/bronze/transactions/')
members_bronze = spark.read.format("delta").load('/mnt/adbquickstart-target/bronze/members/')
user_logs_bronze = spark.read.format("delta").load('/mnt/adbquickstart-target/bronze/user_logs/')

##Write the Bronze to Silver Location
transactions_bronze.write.format('delta').mode('overwrite').save('/mnt/adbquickstart-target/silver/transactions/')
members_bronze.write.format('delta').mode('overwrite').save('/mnt/adbquickstart-target/silver/members/')
user_logs_bronze.write.format('delta').mode('overwrite').save('/mnt/adbquickstart-target/silver/user_logs/')

##Read Silver Data
transactions_silver = spark.read.format("delta").load('/mnt/adbquickstart-target/silver/transactions/')
members_silver = spark.read.format("delta").load('/mnt/adbquickstart-target/silver/members/')
user_logs_silver = spark.read.format("delta").load('/mnt/adbquickstart-target/silver/user_logs/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold table
# MAGIC #### Further we are concentrating on Members dataset. We will create a Gold table (Aggregated table)

# COMMAND ----------

# DBTITLE 1,Members by Registration Year - Create a Gold table
import pyspark.sql.functions as f
members_silver = members_silver.withColumn('years',f.year(f.to_timestamp('registration_init_time', 'yyyy-MM-dd')))

members_gold = members_silver.groupBy('years').count()

members_gold.createOrReplaceTempView("member_gold")

display(members_gold)

# COMMAND ----------

# MAGIC %python
# MAGIC # Save our Gold table in Delta format
# MAGIC members_gold.write.format('delta').mode('overwrite').save('/mnt/adbquickstart-target/gold/members/')
# MAGIC 
# MAGIC # Create SQL table
# MAGIC spark.sql(f"CREATE TABLE kkbox.members_gold USING delta LOCATION '/mnt/adbquickstart-target/gold/members/'") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kkbox.members_gold

# COMMAND ----------

# MAGIC %md ## Scenario 3. Delta as Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `members_gold` table
# MAGIC * We will run two streaming queries concurrently against this data and update the table

# COMMAND ----------

# DBTITLE 1,Stop the notebook before the streaming cell, in case of a "run all" 
dbutils.notebook.exit("stop") 

# COMMAND ----------

# Read the insertion of data
members_gold_readStream = spark.readStream.format("delta").load('/mnt/adbquickstart-target/gold/members/')
members_gold_readStream.createOrReplaceTempView("members_gold_readStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT years, sum(`count`) AS members
# MAGIC FROM members_gold_readStream
# MAGIC GROUP BY years
# MAGIC ORDER BY years

# COMMAND ----------

import time
i = 1
while i <= 6:
  # Execute Insert statement
  insert_sql = "INSERT INTO kkbox.members_gold VALUES (2004, 450000)"
  spark.sql(insert_sql)
  print('members_gold_delta: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Secnario 4 : Perform DML operations , Schema Evolution and Time Travel
# MAGIC #####Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing data engineers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC #####Let's pick the member's gold data 

# COMMAND ----------

# MAGIC %md ### A. DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table to remove records from year 2009
# MAGIC DELETE FROM kkbox.members_gold WHERE years = 2009

# COMMAND ----------

# DBTITLE 1,Let's confirm the data is deleted for year 2009
# MAGIC %sql
# MAGIC SELECT * FROM kkbox.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md ### B. UPDATE Support

# COMMAND ----------

# DBTITLE 1,Let's update the count for year 2010
# MAGIC %sql
# MAGIC UPDATE kkbox.members_gold SET `count` = 50000 WHERE years = 2010

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kkbox.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md ### C. MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC 
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

# DBTITLE 1,Let's create a simple table to merge
items = [(2009, 50000), (2021, 250000), (2012, 35000)]
cols = ['years', 'count']
merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO kkbox.members_gold as d
# MAGIC USING merge_table as m
# MAGIC on d.years = m.years
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Perfect!! Let's check to make sure it worked
# MAGIC %sql
# MAGIC SELECT * FROM kkbox.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# DBTITLE 1,# Generate a new "usage" column in a dummy table
member_dummy = sql("SELECT years, count, CAST(rand(10) * 10 * count AS double) AS usage FROM kkbox.members_gold")
display(member_dummy)

# COMMAND ----------

# DBTITLE 1,Merge it to the delta table
# MAGIC %python
# MAGIC # Add the mergeSchema option
# MAGIC member_dummy.write.option("mergeSchema","true").format("delta").mode("append").save('/mnt/adbquickstart-target/gold/members/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kkbox.members_gold

# COMMAND ----------

# MAGIC %md ## E. Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY kkbox.members_gold

# COMMAND ----------

# MAGIC %md ###  Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# DBTITLE 1,Let's look at the version 0 - When the table was created
# MAGIC %sql
# MAGIC SELECT * FROM kkbox.members_gold VERSION AS OF 0
# MAGIC order by years

# COMMAND ----------

# MAGIC %md ###  OPTIMIZE (Delta Lake on Databricks)
# MAGIC Optimizes the layout of Delta Lake data. Optionally optimize a subset of data or colocate data by column. If you do not specify colocation, bin-packing optimization is performed.

# COMMAND ----------

# MAGIC %sql OPTIMIZE kkbox.user_logs ZORDER BY (date)

# COMMAND ----------

dbutils.fs.unmount('/mnt/adbquickstart')
