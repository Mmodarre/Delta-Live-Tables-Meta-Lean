import datetime
from pyspark.sql.functions import current_user
import helpers.populate_md as pm

dataFlowId = '100-Customers'
dataFlowGroup = "B1"
sourceFormat = "cloudFiles"
sourceDetails = {"path":"/Volumes/mehdidatalake_catalog/retail_cdc/retail_landing/cdc_raw/customers","source_database":"customers","source_table":"customers"}
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
targetDetails = {"database":"mehdidatalake_catalog.retail_cdc","table":"customers_dlt_meta"}
tableProperties = None
partitionColumns = None
cdcApplyChanges = None
dataQualityExpectations = '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
version = "v1"
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = current_user()
updatedBy = current_user()
BRONZE_MD_TABLE = 'mehdidatalake_catalog.dlt_meta_dataflowspecs_1.b_test'



pm.populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,version,createDate,createdBy,updateDate,updatedBy)