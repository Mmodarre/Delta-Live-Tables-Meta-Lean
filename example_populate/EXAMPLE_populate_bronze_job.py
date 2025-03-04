# Databricks notebook source
# MAGIC %md
# MAGIC # Note
# MAGIC - Sample for populating the Bronze Metadata Table.
# MAGIC - This Notebook is designed to run in a Databricks workflow when deployed through CI/CD. 
# MAGIC

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

from dlt_helpers.populate_md import populate_bronze
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------

dataFlowId = '100-Customers' # Dataflow ID -PK
dataFlowGroup = "B1" # Dataflow Group -PK ## ALL the dataflows with the same group are executed in the same DLT pipeline
sourceFormat = "cloudFiles" # Format of the source, for files on Cloud storage use "cloudFiles"
sourceDetails = {
        "path":f"/Volumes/{target_catalog}{env}/{target_schema}/retail_landing/cdc_raw/customers",
        "source_database":"customers",
        "source_table":"customers"
    } #Source Details, If "cloudFiles" the database and table are ignore and are only stored in MD table for reference
highWaterMark = None # High Water Mark for the dataflow, if not provided, It will be populating as NULL {"contract_id":"abc123-456-cfd","contract_version":"1.000","contract_major_version":"1","watermark_column": "operation_date"}
readerConfigOptions ={
        "cloudFiles.format": "json",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFile.readerCaseSensitive": "false",
        "cloudFiles.useNotifications": "false"
    } ## Reader Configuration - Refer to DB documentation https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html
cloudFileNotificationsConfig = None ## If using CloudFilesNotification Mode
# {
#         "cloudFiles.subscriptionId": "az_subscription_id",
#         "cloudFiles.tenantId": "adls_tenant_id_key_name",
#         "cloudFiles.resourceGroup": "az_resource_group",
#         "cloudFiles.clientId": "adls_client_id_key_name",
#         "cloudFiles.clientSecret": "adls_secret_key_name",
#         "configJsonFilePath": "dbfs:/FileStore/Mehdi_Modarressi/config/config-2.json",
#         "kvSecretScope": "key_vault_name"
#     }
schema = None # If inpute schema is defined, rather than the infer schema option
targetFormat = 'delta' ## Target format, almost always "delta"
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"customers_dlt_meta"}
tableProperties = None 
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
cdcApplyChanges = None # Example:  #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}' Documentation: https://docs.databricks.com/en/delta-live-tables/cdc.html
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None 
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy =  spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.bronze_dataflowspec_table" # type: ignore

## Populate Bronze function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.

populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,highWaterMark,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,liquidClusteringColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)