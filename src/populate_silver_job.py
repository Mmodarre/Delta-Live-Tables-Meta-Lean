# Databricks notebook source
dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('schema',defaultValue='_meta')
# COMMAND ----------

catalog =dbutils.widgets.get("catalog")
# COMMAND ----------

env =dbutils.widgets.get("env")
# COMMAND ----------

db = dbutils.widgets.get("schema")

# COMMAND ----------

from dlt_helpers.populate_md import populate_silver
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------

dataFlowId = '100-Customers'
dataFlowId = '110-Customers'
dataFlowGroup = "B1"
sourceFormat = "delta"
sourceDetails = {"database":"mehdidatalake_catalog"+dbutils.widgets.get("env")+".retail_cdc","table":"customers_dlt_meta"}
readerConfigOptions = None
targetFormat = 'delta'
targetDetails = {"database":"mehdidatalake_catalog"+dbutils.widgets.get("env")+".retail_cdc","table":"customers_dlt_meta_silver"}
tableProperties = None
selectExp = ["cast(customers_id as String) as customer_id","address","email","firstname as first_name","lastname as last_name","operation","TO_TIMESTAMP(operation_date, 'MM-dd-yyyy HH:mm:ss') as operation_date","file_path","current_timestamp() as processing_time"]
whereClause = None
partitionColumns = None
cdcApplyChanges = None #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}'
materiazedView = None
dataQualityExpectations = '{"expect_or_drop": {"valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{catalog}{env}.{db}.silver_dataflowspec_table" # type: ignore


populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)