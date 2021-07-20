# Databricks notebook source
# MAGIC %md
# MAGIC ## To run this notebook, you need DBR 8.3+ 

# COMMAND ----------

# DBTITLE 1,Reference Lib
# MAGIC %md
# MAGIC **delta-spark** is fully documented here : https://pypi.org/project/delta-spark/
# MAGIC No installation needed

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists deltademo;
# MAGIC use deltademo;

# COMMAND ----------

# DBTITLE 1,Create table using delta-spark python library
from delta import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

transactions_table_path = "<path-to-store-delta-table-data>"

# Create table in the metastore
transactionsTable = DeltaTable.createIfNotExists(spark) \
    .tableName("transactions") \
    .comment('Fed daily with csv files uploaded from source systems') \
    .location(transactions_table_path) \
    .addColumn('msno', StringType()) \
    .addColumn('payment_method_id', IntegerType()) \
    .addColumn('payment_plan_days', IntegerType()) \
    .addColumn('plan_list_price', IntegerType()) \
    .addColumn('actual_amount_paid', IntegerType()) \
    .addColumn('is_auto_renew', IntegerType(),comment = 'by default set to 0 in source system') \
    .addColumn('transaction_date', DateType()) \
    .addColumn('membership_expire_date', DateType()) \
    .addColumn("gap", dataType = DateType(), generatedAlwaysAs = "transaction_date - 30") \
    .addColumn('is_cancel', IntegerType()) \
    .property('created.by.user','amine.benhamza@databricks.com') \
    .property('quality', 'bronze') \
    .property('delta.autoOptimize.optimizeWrite','true') \
    .property('delta.autoOptimize.autoCompact','true') \
    .partitionedBy('transaction_date') \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED deltademo.transactions;

# COMMAND ----------

# DBTITLE 1,Retrieve Data from source
# MAGIC %sh
# MAGIC filename="/dbfs/tmp/transactions_v3.csv"
# MAGIC src="https:https://raw.githubusercontent.com/amineds/datafiles/main/transactions_v3.csv"
# MAGIC 
# MAGIC wget -O "$filename" "$src"

# COMMAND ----------

# DBTITLE 1,Check file existence
data = "/tmp/transactions_v3.csv"
dbutils.fs.ls(data)

# COMMAND ----------

# DBTITLE 1,Read raw data in csv format
from pyspark.sql.types import *
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

url = "https://raw.githubusercontent.com/amineds/datafiles/main/transactions_v3.csv"
from pyspark import SparkFiles
spark.sparkContext.addFile(url)

# read data from csv
transactions = spark.read.csv("/tmp/transactions_v3.csv",schema=transaction_schema,header=False,dateFormat='yyyyMMdd')

# COMMAND ----------

# DBTITLE 1,Persist in delta lake format
transactions.write.partitionBy('transaction_date').mode('overwrite').saveAsTable('deltademo.transactions')

# COMMAND ----------

# DBTITLE 1,Get a reference to Delta table using delta-spark library
myTransactionsTable = DeltaTable.forName(spark,"deltademo.transactions")

# COMMAND ----------

# DBTITLE 1,Delete Data
#option is set to avoid errors while deleting recent files from Delta table
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")

# COMMAND ----------

myTransactionsTable.delete("transaction_date='2015-01-01'")

# COMMAND ----------

# DBTITLE 1,Check row count
# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from deltademo.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from deltademo.transactions 
# MAGIC VERSION AS OF 1

# COMMAND ----------

# DBTITLE 1,Restore table and query history
myTransactionsTable.restoreToVersion(1)

# COMMAND ----------

display(myTransactionsTable.history())

# COMMAND ----------

#check data recovery
%sql
select * from deltademo.transactions
where transaction_date = '2015-01-01'

# COMMAND ----------

# DBTITLE 1,Update data
myTransactionsTable.update(
    condition = "transaction_date = '2015-01-01'",
    set = { "payment_method_id": "99" } )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltademo.transactions
# MAGIC where transaction_date = '2015-01-01'

# COMMAND ----------

# DBTITLE 1,Use Spark Dataframe to explore data
transactionsDF = myTransactionsTable.toDF()

# COMMAND ----------

display(transactionsDF.filter("transaction_date='2015-01-01'"))
