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

dataFlowId = '100-Customers'
dataFlowGroup = "B1"
sourceFormat = "cloudFiles"
sourceDetails = {
        "path":f"/Volumes/{target_catalog}{env}/{target_schema}/retail_landing/cdc_raw/customers",
        "source_database":"customers",
        "source_table":"customers"
    }
readerConfigOptions ={
        "cloudFiles.format": "json",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFile.readerCaseSensitive": "false",
        "cloudFiles.useNotifications": "false"
    }
cloudFileNotificationsConfig = None
# {
#         "cloudFiles.subscriptionId": "az_subscription_id",
#         "cloudFiles.tenantId": "adls_tenant_id_key_name",
#         "cloudFiles.resourceGroup": "az_resource_group",
#         "cloudFiles.clientId": "adls_client_id_key_name",
#         "cloudFiles.clientSecret": "adls_secret_key_name",
#         "configJsonFilePath": "dbfs:/FileStore/Mehdi_Modarressi/config/config-2.json",
#         "kvSecretScope": "key_vault_name"
#     }
schema = None
targetFormat = 'delta'
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"customers_dlt_meta"}
tableProperties = None
partitionColumns = None
cdcApplyChanges = None
dataQualityExpectations = '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy =  spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.bronze_dataflowspec_table" # type: ignore



populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)