# Databricks notebook source
# DBTITLE 1,Access using Delta Reader
from deltalake import DeltaTable
from adlfs import AzureBlobFileSystem

#example url  'abfss://myContainer@myStorageAccount.dfs.core.windows.net/somepath/mytable'
fs = AzureBlobFileSystem(
        account_name="aminebenmtc", 
        credential='7/iyZ/JC2YsBEPbddBubHfuK8mVDzTBvNXtjQ+mBwX30v5d7eDs77+jw/7e86OHVjhwv/X3MRa1b5gGTyQfVbA=='
    )

print("import and fs done")

df_full = DeltaTable("kkbox/transactions/delta", file_system=fs).to_pandas()

# COMMAND ----------

df_full

# COMMAND ----------

import pyarrow.dataset as ds

#Predicate pushdown. 
#If the table is partitioned on age, it will also to partition pruning
df_filtered = DeltaTable("kkbox/transactions/delta", file_system=fs) \
              .to_table(columns=["payment_method_id","payment_plan_days"],filter=ds.field("transaction_date")=="2015-01-01") \
              .to_pandas()

# COMMAND ----------

df_filtered
