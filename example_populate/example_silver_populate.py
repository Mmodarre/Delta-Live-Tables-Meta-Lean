#Databricks notebook source
# COMMAND ----------

dbutils.widgets.text('env',defaultValue='_dev')

# COMMAND ----------

import datetime
import dlt_helpers.populate_md as pm
from pyspark.sql.functions import current_user

# COMMAND ----------

dataFlowId = '110-Customers'
dataFlowGroup = "B1"
sourceFormat = "delta"
sourceDetails = {"database":"mehdidatalake_catalog"+ dbutils.widgets.get('env') +".retail_cdc","table":"customers_dlt_meta"}
readerConfigOptions = None
targetFormat = 'delta'
targetDetails = {"database":"mehdidatalake_catalog"+ dbutils.widgets.get('env') +".retail_cdc","table":"customers_dlt_meta_silver"}
tableProperties = None
selectExp = ["cast(customers_id as String) as customer_id","address","email","firstname as first_name","lastname as last_name","operation","TO_TIMESTAMP(operation_date, 'MM-dd-yyyy HH:mm:ss') as operation_date","file_path","current_timestamp() as processing_time"]
whereClause = None
partitionColumns = None
cdcApplyChanges = '{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}'
dataQualityExpectations = '{"expect_or_drop": {"valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = "mehdidatalake_catalog"+ dbutils.widgets.get('env')+"._meta.silver_dataflowspec_table" # type: ignore


pm.populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)