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

# Create table in the metastore
transactionsTable = DeltaTable.createIfNotExists(spark) \
    .tableName("transactions") \
    .comment('Fed daily with csv files uploaded from source systems') \
    .location("/mnt/kkbox/transactions/delta") \
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

# read data from csv
transactions = spark.read.csv('/mnt/kkbox/transactions/transactions_v2.csv',schema=transaction_schema,header=True,dateFormat='yyyyMMdd')

# COMMAND ----------

# persist in delta lake format
transactions.write.partitionBy('transaction_date').mode('overwrite').saveAsTable('amineben_delta.transactions')

# COMMAND ----------

import shutil
_ = spark.sql('drop table amineben_delta.transactions')
shutil.rmtree('/mnt/kkbox/transactions/delta', ignore_errors=True)

# COMMAND ----------

transactionsTable = DeltaTable.forname("amineben_delta.transactions")

# COMMAND ----------

# DBTITLE 1,This option is set to avoid errors while deleting recent files from Delta table
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")

# COMMAND ----------

transactionsTable.delete("transaction_date='2015-01-01'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from amineben_delta.transactions

# COMMAND ----------

transactionsTable.restoreToVersion(1)

# COMMAND ----------

display(transactionsTable.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amineben_delta.transactions
# MAGIC where transaction_date = '2015-01-01'

# COMMAND ----------

transactionsTable.update(
    condition = "transaction_date = '2015-01-01'",
    set = { "payment_method_id": "99" } )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amineben_delta.transactions
# MAGIC where transaction_date = '2015-01-01'

# COMMAND ----------

transactionsDF = transactionsTable.toDF()

# COMMAND ----------

display(transactionsDF.filter("transaction_date='2015-01-01'"))
