# Databricks notebook source
# MAGIC %md
# MAGIC ## To run this notebook, you would need DBR 8.3+ 

# COMMAND ----------

# DBTITLE 1,Reference Lib
# MAGIC %md
# MAGIC 
# MAGIC **delta-lake-reader** is fully documented here : https://pypi.org/project/delta-lake-reader/
# MAGIC 
# MAGIC To access data in Azure, install the relevant library variant : delta-lake-reader[azure]. See here for [instructions](https://docs.microsoft.com/en-us/azure/databricks/libraries/cluster-libraries) : 

# COMMAND ----------

# DBTITLE 1,Access using Delta Reader
from deltalake import DeltaTable
from adlfs import AzureBlobFileSystem

#example url  'abfss://myContainer@myStorageAccount.dfs.core.windows.net/somepath/mytable'
fs = AzureBlobFileSystem(
        account_name='<myStorageAccount>', 
        credential='<access-key>'
    )

df_full = DeltaTable("<path-to-data>", file_system=fs).to_pandas()

# COMMAND ----------

df_full

# COMMAND ----------

import pyarrow.dataset as ds

#Predicate pushdown. 
#If the table is partitioned on age, it will also to partition pruning
df_filtered = DeltaTable("<path-to-data>", file_system=fs) \
              .to_table(columns=["payment_method_id","payment_plan_days"],filter=ds.field("transaction_date")=="2015-01-01") \
              .to_pandas()

# COMMAND ----------

df_filtered
