# Databricks notebook source
# MAGIC %md
# MAGIC # Note
# MAGIC - Sample for populating the Silver Metadata Table.
# MAGIC - This Notebook is designed to run in a Databricks workflow when deployed through CI/CD. 

# COMMAND ----------

dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Schema',defaultValue='edw_bluebikes_ebikes_bronze')

# COMMAND ----------

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')

# COMMAND ----------

target_catalog =dbutils.widgets.get("Target_Catalog")

# COMMAND ----------

target_schema = dbutils.widgets.get("Target_Schema")

# COMMAND ----------

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

# COMMAND ----------

meta_schema = dbutils.widgets.get("Metadata_Schema")

# COMMAND ----------

env =dbutils.widgets.get("env")

# COMMAND ----------

from dlt_helpers.populate_md import populate_silver
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------


dataFlowId = '001-ebikes_at_station' # Unique ID for the dataflow -- PK
dataFlowGroup = "BBB_Silver" # Dataflow group ID -- PK
sourceFormat = "delta" # Reading from Bronze Layer Delta Table
sourceDetails = {"source_database" : f"mehdidatalake_catalog{env}.edw_bluebikes_ebikes_bronze","source_table": "ebikes_at_station_bronze_dlt_meta"}  #Source Table Details
readerConfigOptions = None 
targetFormat = 'delta' # F
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"ebikes_at_station_silver_dlt_meta"}
tableProperties = None
selectExp = None
whereClause = None
partitionColumns = None
cdcApplyChanges = None #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}'
materiazedView = None
## Example of how to create a SQL Statement
## For Materialized Views.
# """
# SELECT
#     last_updated,
#     ttl,
#     version,
#     station.station_id,
#     ebike.battery_charge_percentage,
#     ebike.displayed_number,
#     ebike.docking_capability,
#     ebike.is_lbs_internal_rideable,
#     ebike.make_and_model,
#     ebike.range_estimate.conservative_range_miles AS conservative_range_miles,
#     ebike.range_estimate.estimated_range_miles AS estimated_range_miles,
#     ebike.rideable_id
# FROM
#     mehdidatalake_catalog.retail_cdc.ebikes_at_station_bronze_dlt_meta
# LATERAL VIEW explode(data.stations) AS station
# LATERAL VIEW explode(station.ebikes) AS ebike
# """
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.silver_dataflowspec_table" # type: ignore

<<<<<<< Updated upstream

populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)
=======
## Populate silver function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.
populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)
>>>>>>> Stashed changes
