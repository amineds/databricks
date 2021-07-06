# Databricks notebook source
# MAGIC %md
# MAGIC <!-- You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well. -->
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 7.6 ML or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC For the bottom sections of this notebook, you will need `yellowbrick` installed on your cluster as well

# COMMAND ----------

db = "delta_adb_essentials"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
# spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false")

# COMMAND ----------

# MAGIC %md # Getting started with <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC An open-source storage layer for data lakes that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Open Format**: Stored as Parquet format in blob storage.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.

# COMMAND ----------

# MAGIC %md ## Import Data and create pre-Delta Lake Table
# MAGIC * This will create a lot of small Parquet files emulating the typical small file problem that occurs with streaming or highly transactional data

# COMMAND ----------

# DBTITLE 0,Import Data and create pre-Databricks Delta Table
# -----------------------------------------------
# Uncomment and run if this folder does not exist
# -----------------------------------------------
# Configure location of loanstats_2012_2017.parquet
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
loan_stats = spark.read.parquet(lspq_path)

# Select only the columns needed
#loan_stats = loan_stats.select("addr_state", "loan_status")

# Create loan by state
loan_by_state = loan_stats.select("addr_state", "loan_status").groupBy("addr_state").count()

# Create table
loan_by_state.createOrReplaceTempView("loan_by_state")

# Display loans by state
display(loan_by_state)

# COMMAND ----------

# MAGIC %md Delta Lake is 100% compatible with Apache Spark&trade;, which makes it easy to get started with if you already use Spark for your big data workflows.
# MAGIC Delta Lake features APIs for **SQL**, **Python**, and **Scala**, so that you can use it in whatever language you feel most comfortable in.

# COMMAND ----------

# MAGIC %md <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=1012/>

# COMMAND ----------


# Configure Delta Lake Silver Path
DELTALAKE_SILVER_PATH = "/adbessentials/loan_by_state_delta"

# Remove folder if it exists
dbutils.fs.rm(DELTALAKE_SILVER_PATH, recurse=True)
dbutils.fs.rm("/adbessentials/loan_stats", recurse=True)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Convert to Delta Lake format

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Current example is creating a new table instead of in-place import so will need to change this code
# MAGIC DROP TABLE IF EXISTS loan_by_state_delta;
# MAGIC 
# MAGIC CREATE TABLE loan_by_state_delta
# MAGIC USING delta
# MAGIC LOCATION '/adbessentials/loan_by_state_delta'
# MAGIC AS SELECT * FROM loan_by_state;
# MAGIC 
# MAGIC -- View Delta Lake table
# MAGIC SELECT * FROM loan_by_state_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL delta.`/adbessentials/loan_by_state_delta`

# COMMAND ----------

loan_stats.write.format("delta").save("/adbessentials/loan_stats")

spark.sql("DROP TABLE IF EXISTS loan_stats")

spark.sql("CREATE TABLE loan_stats USING DELTA LOCATION '/adbessentials/loan_stats'")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table delta_adb_essentials.loan_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail loan_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the notebook before the streaming cell, in case of a "run all" 

# COMMAND ----------

dbutils.notebook.exit("stop") 

# COMMAND ----------

# MAGIC %fs ls /adbessentials/loan_by_state_delta/

# COMMAND ----------

# MAGIC %fs ls /adbessentials/loan_stats/

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified batch + streaming data processing with concurrent readers and writers
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `loan_stats_delta` table
# MAGIC * We will run two streaming queries concurrently against this data

# COMMAND ----------

# Read the insertion of data
loan_by_state_readStream = spark.readStream.format("delta").load(DELTALAKE_SILVER_PATH)
loan_by_state_readStream.createOrReplaceTempView("loan_by_state_readStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_readStream group by addr_state

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running before executing the code below

# COMMAND ----------

import time
i = 1
while i <= 6:
  # Execute Insert statement
  insert_sql = "INSERT INTO loan_by_state_delta VALUES ('IA', 45000)"
  spark.sql(insert_sql)
  print('loan_by_state_delta: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(5)

# COMMAND ----------

# MAGIC %fs ls /adbessentials/loan_by_state_delta/_delta_log/

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Once the previous cell is finished and the state of Iowa is fully populated in the map (in cell 14), click *Cancel* in Cell 14 to stop the `readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's review our current set of loans using our map visualization.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %md Observe that the Iowa (middle state) has the largest number of loans due to the recent stream of data.  Note that the original `loan_by_state_delta` table is updated as we're reading `loan_by_state_readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support: `DELETE`, `UPDATE`, `MERGE INTO`
# MAGIC 
# MAGIC Delta Lake brings ACID transactions and full DML support to data lakes.
# MAGIC 
# MAGIC >Parquet does **not** support these commands - they are unique to Delta Lake.

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

# Load new DataFrame based on current Delta table
lbs_df = sql("select * from loan_by_state_delta")

# Save DataFrame to Parquet
lbs_df.write.mode("overwrite").parquet("/tmp/loan_by_state.parquet")

# Create new table on this parquet data
spark.sql("drop table if exists loan_by_state_pq")
spark.sql("create table loan_by_state_pq using parquet as select * from parquet.`/tmp/loan_by_state.parquet`")

# Review data
display(sql("select * from loan_by_state_pq"))

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC 
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `DELETE` those values assigned to `IA`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Parquet table
# MAGIC DELETE FROM loan_by_state_pq WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table
# MAGIC DELETE FROM loan_by_state_delta WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `UPDATE` those values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Parquet table
# MAGIC UPDATE loan_by_state_pq SET `count` = 2700 WHERE addr_state = 'WA'

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `UPDATE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `UPDATE` on the Delta Lake table
# MAGIC UPDATE loan_by_state_delta SET `count` = 270000 WHERE addr_state = 'WA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Let's create a simple table to merge
items = [('IA', 10), ('CA', 2500), ('OR', None)]
cols = ['addr_state', 'count']
merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO loan_by_state_delta as d
# MAGIC USING merge_table as m
# MAGIC on d.addr_state = m.addr_state
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Evolution to add new columns to schema
# MAGIC 
# MAGIC If we *want* to update our Delta Lake table to match this data source's schema, we can do so using schema evolution. Simply add the following to the Spark write command: `.option("mergeSchema", "true")`

# COMMAND ----------

# Generate new loans with dollar amounts 
loans = sql("select addr_state, cast(rand(10)*count as bigint) as count, cast(rand(10) * 10000 * count as double) as amount from loan_by_state_delta")
display(loans)

# COMMAND ----------

# Let's write this data out to our Delta table
loans.write.format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the schema of our new data does not match the schema of our original data

# COMMAND ----------

# Add the mergeSchema option
loans.write.option("mergeSchema","true").format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

# COMMAND ----------

# MAGIC %md **Note**: With the `mergeSchema` option, we can merge these different schemas together.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`amount`) as amount from loan_by_state_delta group by addr_state order by sum(`amount`) desc limit 10

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Time Travel

# COMMAND ----------

# MAGIC %md Delta Lake’s time travel capabilities simplify building data pipelines for use cases including:
# MAGIC 
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC <img src="https://github.com/risan4841/img/blob/master/transactionallogs.png?raw=true" width=250/>
# MAGIC 
# MAGIC You can query snapshots of your tables by:
# MAGIC 1. **Version number**, or
# MAGIC 2. **Timestamp.**
# MAGIC 
# MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to the [docs](https://docs.delta.io/latest/delta-utility.html#history), or [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md #### Review Delta Lake Table History for  Auditing & Governance
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY loan_by_state_delta

# COMMAND ----------

# MAGIC %md #### Use time travel to select and view the original version of our table (Version 0).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loan_by_state_delta VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loan_by_state_delta VERSION AS OF 9

# COMMAND ----------

# MAGIC %md #### Rollback a table to a specific version using `RESTORE`

# COMMAND ----------

# %sql RESTORE loans_delta VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Problem Statement: Classifying "bad loans" for a lender
# MAGIC 
# MAGIC This notebook tackles a classification problem on the Lending Club dataset, with the goal of identifying "bad loans" (loans likely to be unprofitable) based on a combination of credit scores, credit history, and other features.
# MAGIC 
# MAGIC The end goal is to produce an interpretable model that a loan officer can use before deciding whether to approve a loan. Such a model provides an informative view for the lender as well as an immediate estimate and response for the prospective borrower.

# COMMAND ----------

# DBTITLE 1,Create Delta Table for Machine Learning experiments
from pyspark.sql.functions import *

# Remove table if it exists
DELTA_TABLE_ML_PATH = "/adbessentials/loan_stats_ml"
dbutils.fs.rm(DELTA_TABLE_ML_PATH, recurse=True)

# read delta data loan stats
data = spark.read.format("delta").load("/adbessentials/loan_stats")

# Select only the columns needed & apply other preprocessing
features = ["loan_amnt",  "annual_inc", "dti", "delinq_2yrs","total_acc", "total_pymnt", "issue_d", "earliest_cr_line"]
raw_label = "loan_status"
loan_stats_ml = data.select(*(features + [raw_label]))
print("------------------------------------------------------------------------------------------------")
print("Create bad loan label, this will include charged off, defaulted, and late repayments on loans...")
loan_stats_ml = loan_stats_ml.filter(loan_stats_ml.loan_status.isin(["Default", "Charged Off", "Fully Paid"]))\
                       .withColumn("bad_loan", (~(loan_stats_ml.loan_status == "Fully Paid")).cast("string"))
loan_stats_ml = loan_stats_ml.orderBy(rand()).limit(10000) # Limit rows loaded to facilitate running on Community Edition
print("------------------------------------------------------------------------------------------------")
print("Casting numeric columns into the appropriate types...")
loan_stats_ml = loan_stats_ml.withColumn('issue_year',  substring(loan_stats_ml.issue_d, 5, 4).cast('double')) \
                       .withColumn('earliest_year', substring(loan_stats_ml.earliest_cr_line, 5, 4).cast('double')) \
                       .withColumn('total_pymnt', loan_stats_ml.total_pymnt.cast('double'))
loan_stats_ml = loan_stats_ml.withColumn('credit_length_in_years', (loan_stats_ml.issue_year - loan_stats_ml.earliest_year))   

# Save table in Delta Lake format
loan_stats_ml.write.format("delta").mode("overwrite").save(DELTA_TABLE_ML_PATH)
display(loan_stats_ml)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Train a Model with Cross Validation for Hyperparameter Tuning
# MAGIC Train an ML pipeline using Spark MLlib. The metrics and params from your tuning runs are automatically tracked to MLflow for later inspection.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder, StandardScaler, Imputer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import mlflow.spark
from pyspark.sql import SparkSession

# Use autolog API to automatically log parameters including data_version, data_path
mlflow.spark.autolog()

def _fit_crossvalidator(train, features, target):
  """
  Helper function that fits a CrossValidator model to predict a binary label
  `target` on the passed-in training DataFrame using the columns in `features`
  :param: train: Spark DataFrame containing training data
  :param: features: List of strings containing column names to use as features from `train`
  :param: target: String name of binary target column of `train` to predict
  """
  with mlflow.start_run():
      train = train.select(features + [target])
      model_matrix_stages = [
        Imputer(inputCols = features, outputCols = features),
        VectorAssembler(inputCols=features, outputCol="features"),
        StringIndexer(inputCol="bad_loan", outputCol="label")
      ]
      lr = LogisticRegression(maxIter=10, elasticNetParam=0.5, featuresCol = "features")
      pipeline = Pipeline(stages=model_matrix_stages + [lr])
      paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
      crossval = CrossValidator(estimator=pipeline,
                                estimatorParamMaps=paramGrid,
                                evaluator=BinaryClassificationEvaluator(),
                                numFolds=5)

      cvModel = crossval.fit(train)
      mlflow.spark.log_model(cvModel.bestModel,"spark-model")
  return cvModel.bestModel

# COMMAND ----------

# Fit model & display ROC
features = ["loan_amnt",  "annual_inc", "dti", "delinq_2yrs","total_acc", "credit_length_in_years"]
glm_model = _fit_crossvalidator(loan_stats_ml, features, target="bad_loan")
lr_summary = glm_model.stages[len(glm_model.stages)-1].summary
display(lr_summary.roc)

# COMMAND ----------

print("ML Pipeline accuracy: %s" % lr_summary.accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Training Results in the MLflow Experiment Runs sidebar
# MAGIC 
# MAGIC The model training code above automatically logged metrics and params under an MLflow run, which you can view using the [MLflow Runs Sidebar](https://databricks.com/blog/2019/04/30/introducing-mlflow-run-sidebar-in-databricks-notebooks.html). Click Experiment at the upper right to display the Experiment Runs sidebar.

# COMMAND ----------

# MAGIC %md ### Feature Engineering: Evolve Data Schema
# MAGIC 
# MAGIC You can do some feature engineering to potentially improve model performance, using Delta Lake to track older versions of the dataset. First, add a feature tracking the total amount of money earned or lost per loan:

# COMMAND ----------

print("------------------------------------------------------------------------------------------------")
print("Calculate the total amount of money earned or lost per loan...")
loan_stats_ml_new = loan_stats_ml.withColumn('net', round( loan_stats_ml.total_pymnt - loan_stats_ml.loan_amnt, 2))

# COMMAND ----------

# MAGIC %md Save the updated table, passing the `mergeSchema` option to safely evolve its schema.

# COMMAND ----------

loan_stats_ml_new.write.option("mergeSchema", "true").format("delta").mode("overwrite").save(DELTA_TABLE_ML_PATH)

# COMMAND ----------

# See the difference between the original & modified schemas
set(loan_stats_ml_new.schema.fields) - set(loan_stats_ml.schema.fields)

# COMMAND ----------

# MAGIC %md Retrain the model on the updated data and compare its performance to the original.

# COMMAND ----------

# Return ROC
glm_model_new = _fit_crossvalidator(loan_stats_ml_new, features + ["net"], target="bad_loan")
lr_summary_new = glm_model_new.stages[len(glm_model_new.stages)-1].summary
display(lr_summary_new.roc)

# COMMAND ----------

print("ML Pipeline accuracy: %s" % lr_summary_new.accuracy)
