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

schema = dbutils.widgets.get("schema")
# COMMAND ----------

from dlt_helpers.populate_md import populate_bronze
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------

dataFlowId = '100-Customers'
dataFlowGroup = "B1"
sourceFormat = "cloudFiles"
sourceDetails = {"path":f"/Volumes/mehdidatalake_catalog"+dbutils.widgets.get("env")+"/retail_cdc/retail_landing/cdc_raw/customers","source_database":"customers","source_table":"customers"}
readerConfigOptions ={
        "cloudFiles.format": "json",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFile.readerCaseSensitive": "false",
        "cloudFiles.useNotifications": "true"
    }
cloudFileNotificationsConfig = {
        "cloudFiles.subscriptionId": "az_subscription_id",
        "cloudFiles.tenantId": "adls_tenant_id_key_name",
        "cloudFiles.resourceGroup": "az_resource_group",
        "cloudFiles.clientId": "adls_client_id_key_name",
        "cloudFiles.clientSecret": "adls_secret_key_name",
        "configJsonFilePath": "dbfs:/FileStore/Mehdi_Modarressi/config/config-2.json",
        "kvSecretScope": "key_vault_name"
    }
schema = None
targetFormat = 'delta'
targetDetails = {"database":"mehdidatalake_catalog"+dbutils.widgets.get("env")+".retail_cdc","table":"customers_dlt_meta"}
tableProperties = None
partitionColumns = None
cdcApplyChanges = None
dataQualityExpectations = '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
BRONZE_MD_TABLE = f"{catalog}{env}.{schema}.bronze_dataflowspec_table" # type: ignore



populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)