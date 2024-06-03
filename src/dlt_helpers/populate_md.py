from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    ArrayType,
    TimestampType
)
from pyspark.sql.functions import *
from delta.tables import *

## Function to get the next version of the pipeline
## 
## @param BRONZE_MD_TABLE: The name of the Bronze Metadata table
## @param dataFlowId: The unique identifier for the pipeline
## @param dataFlowGroup: The group to which the pipeline belongs
## @param spark: The SparkSession object
def get_next_version(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,spark):
  ## Get the current version of the pipeline from MD table
  current_version = spark.sql('select version from '+BRONZE_MD_TABLE+' where dataFlowId="'+dataFlowId+'" and dataFlowGroup = "'+dataFlowGroup+'"').collect()
  
  ## If the pipeline is already in the MD table, increment the version by 1
  if len(current_version) >0:
    return "v"+str(int(current_version[0]['version'][1:])+1)
  ## If the pipeline is not in the MD table, set the version to v1
  else:
    return "v1"


## Function to populate the Bronze Metadata table
##
## @param BRONZE_MD_TABLE: The name of the Bronze Metadata table
## @param dataFlowId: The unique identifier for the pipeline
## @param dataFlowGroup: The group to which the pipeline belongs
## @param sourceFormat: The format of the source data
## @param sourceDetails: The details of the source data
## @param readerConfigOptions: The configuration options for the reader
## @param cloudFileNotificationsConfig: The configuration options for cloud file notifications
## @param schema: The schema of the source data
## @param targetFormat: The format of the target data
## @param targetDetails: The details of the target data
## @param tableProperties: The properties of the target table
## @param partitionColumns: The partition columns for the target table
## @param cdcApplyChanges: The CDC configuration for the pipeline
## @param dataQualityExpectations: The data quality expectations for the pipeline
## @param quarantineTargetDetails: The details of the quarantine target
## @param quarantineTableProperties: The properties of the quarantine table
## @param createDate: The creation date of the pipeline
## @param createdBy: The user who created the pipeline
## @param updateDate: The last update date of the pipeline
## @param updatedBy: The user who last updated the pipeline
## @param spark: The SparkSession object
##
## @return None
##
## @example
'''
dbutils.widgets.text('env',defaultValue='_dev')
dataFlowId = '110-Customers'
dataFlowGroup = "B1"+dbutils.widgets.get('env')
sourceFormat = "cloudFiles"
sourceDetails = {"path":"/Volumes/mehdidatalake_catalog"+dbutils.widgets.get("env")+"/retail_cdc/retail_landing/cdc_raw/customers","source_database":"customers","source_table":"customers"}
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
BRONZE_MD_TABLE = "mehdidatalake_catalog"+ dbutils.widgets.get('env') +"._meta.bronze_dataflowspec_table"



pm.populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)

'''
## @example

'''
import datetime
from pyspark.sql.functions import current_user
import dlt_helpers.populate_md as pm

dbutils.widgets.text('env',defaultValue='_dev')

dataFlowId = '100-Customers'
dataFlowGroup = "B1"
sourceFormat = "cloudFiles"
sourceDetails = {"path":"/Volumes/mehdidatalake_catalog"+ dbutils.widgets.get('env') +"/retail_cdc/retail_landing/cdc_raw/customers","source_database":"customers","source_table":"customers"}
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
targetDetails = {"database":"mehdidatalake_catalog"+ dbutils.widgets.get('env') +".retail_cdc","table":"customers_dlt_meta"}
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
BRONZE_MD_TABLE = "mehdidatalake_catalog"+ dbutils.widgets.get('env') +"._meta.bronze_dataflowspec_table"



pm.populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy)
'''
def populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark):

  schema_definition = StructType([
    StructField('dataFlowId', StringType(), True),
    StructField('dataFlowGroup', StringType(), True),
    StructField('sourceFormat', StringType(), True),
    StructField('sourceDetails', MapType(StringType(), StringType(), True), True),
    StructField('readerConfigOptions', MapType(StringType(), StringType(), True), True),
    StructField('cloudFileNotificationsConfig', MapType(StringType(), StringType(), True), True),
    StructField('schema',StringType(),True),
    StructField('targetFormat', StringType(), True),
    StructField('targetDetails', MapType(StringType(), StringType(), True), True),
    StructField('tableProperties', MapType(StringType(), StringType(), True), True),
    StructField('partitionColumns', ArrayType(StringType(), True), True),
    StructField('cdcApplyChanges', StringType(), True),
    StructField('dataQualityExpectations', StringType(), True),
    StructField('quarantineTargetDetails', MapType(StringType(), StringType(), True), True),
    StructField('quarantineTableProperties', MapType(StringType(), StringType(), True), True),
    StructField('createDate', TimestampType(), True),
    StructField('createdBy', StringType(), True),
    StructField('updateDate', TimestampType(), True),
    StructField('updatedBy', StringType(), True)
])
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions,cloudFileNotificationsConfig, schema, targetFormat, targetDetails, tableProperties,partitionColumns, cdcApplyChanges, dataQualityExpectations, quarantineTargetDetails,quarantineTableProperties,createDate, createdBy,updateDate, updatedBy)]
  ## Create a dataframe from the data
  bronze_new_record_df = spark.createDataFrame(data, schema_definition)
  
  ## Get the DeltaTable object for the Bronze Metadata table
  bronze_md_table = DeltaTable.forName(spark,BRONZE_MD_TABLE)

  ## Merge the new record with the existing records in the Bronze Metadata table
  bronze_md_table.alias('bronze_md') \
  .merge(bronze_new_record_df.alias('updates'),'bronze_md.dataFlowId = updates.dataFlowId and bronze_md.dataFlowGroup = updates.dataFlowGroup') \
  .whenMatchedUpdate(set =
    {
      "dataFlowId": "updates.dataFlowId",
      "dataFlowGroup": "updates.dataFlowGroup",
      "sourceFormat": "updates.sourceFormat",
      "sourceDetails": "updates.sourceDetails",
      "readerConfigOptions": "updates.readerConfigOptions",
      "cloudFileNotificationsConfig": "updates.cloudFileNotificationsConfig",
      "schema": "updates.schema",
      "targetFormat": "updates.targetFormat",
      "targetDetails":"updates.targetDetails",
      "tableProperties":"updates.tableProperties",
      "partitionColumns":"updates.partitionColumns",
      "cdcApplyChanges":"updates.cdcApplyChanges",
      "dataQualityExpectations":"updates.dataQualityExpectations",
      "quarantineTargetDetails":"updates.quarantineTargetDetails",
      "quarantineTableProperties":"updates.quarantineTableProperties",
      "version": lit(get_next_version(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,spark)),
      "createDate":"bronze_md.createDate",
      "createdBy":"bronze_md.createdBy",
      "updateDate":"updates.updateDate",
      "updatedBy":"updates.updatedBy"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "dataFlowId": "updates.dataFlowId",
      "dataFlowGroup": "updates.dataFlowGroup",
      "sourceFormat": "updates.sourceFormat",
      "sourceDetails": "updates.sourceDetails",
      "readerConfigOptions": "updates.readerConfigOptions",
      "cloudFileNotificationsConfig": "updates.cloudFileNotificationsConfig",
      "schema": "updates.schema",
      "targetFormat": "updates.targetFormat",
      "targetDetails":"updates.targetDetails",
      "tableProperties":"updates.tableProperties",
      "partitionColumns":"updates.partitionColumns",
      "cdcApplyChanges":"updates.cdcApplyChanges",
      "dataQualityExpectations":"updates.dataQualityExpectations",
      "quarantineTargetDetails":"updates.quarantineTargetDetails",
      "quarantineTableProperties":"updates.quarantineTableProperties",
      "version": lit(get_next_version(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,spark)),
      "createDate":"updates.createDate",
      "createdBy":"updates.createdBy",
      "updateDate":"updates.updateDate",
      "updatedBy":"updates.updatedBy"
    }
  ) \
  .execute()
  

## Function to populate the Silver Metadata table
##
## @param SILVER_MD_TABLE: The name of the Silver Metadata table
## @param dataFlowId: The unique identifier for the pipeline
## @param dataFlowGroup: The group to which the pipeline belongs
## @param sourceFormat: The format of the source data
## @param sourceDetails: The details of the source data
## @param readerConfigOptions: The configuration options for the reader
## @param targetFormat: The format of the target data
## @param targetDetails: The details of the target data
## @param tableProperties: The properties of the target table
## @param selectExp: The select expression for the target table
## @param whereClause: The where clause for the target table
## @param partitionColumns: The partition columns for the target table
## @param cdcApplyChanges: The CDC configuration for the pipeline
## @param dataQualityExpectations: The data quality expectations for the pipeline
## @param createDate: The creation date of the pipeline
## @param createdBy: The user who created the pipeline
## @param updateDate: The last update date of the pipeline
## @param updatedBy: The user who last updated the pipeline
## @param spark: The SparkSession object
##
## @return None
##
## @example
'''
import datetime
import dlt_helpers.populate_md as pm
from pyspark.sql.functions import current_user

dbutils.widgets.text('env',defaultValue='_dev')

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
silver_MD_TABLE = "mehdidatalake_catalog"+ dbutils.widgets.get('env') +"._meta.silver_dataflowspec_table"


pm.populate_silver(silver_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)
'''
def populate_silver(SILVER_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,targetFormat,targetDetails,tableProperties,selectExp,whereClause,partitionColumns,cdcApplyChanges,dataQualityExpectations,createDate,createdBy,updateDate,updatedBy,spark):

  schema_definition = StructType(
        [
            StructField("dataFlowId", StringType(), True),
            StructField("dataFlowGroup", StringType(), True),
            StructField("sourceFormat", StringType(), True),
            StructField("sourceDetails", MapType(StringType(), StringType(), True), True),
            StructField("readerConfigOptions", MapType(StringType(), StringType(), True), True),
            StructField("targetFormat", StringType(), True),
            StructField("targetDetails", MapType(StringType(), StringType(), True), True),
            StructField("tableProperties", MapType(StringType(), StringType(), True), True),
            StructField("selectExp", ArrayType(StringType(), True), True),
            StructField("whereClause", ArrayType(StringType(), True), True),
            StructField("partitionColumns", ArrayType(StringType(), True), True),
            StructField("cdcApplyChanges", StringType(), True),
            StructField("dataQualityExpectations", StringType(), True),
            StructField("createDate", TimestampType(), True),
            StructField("createdBy", StringType(), True),
            StructField("updateDate", TimestampType(), True),
            StructField("updatedBy", StringType(), True),
        ]
    )
  
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,
         selectExp,whereClause,partitionColumns, cdcApplyChanges, dataQualityExpectations
         ,createDate, createdBy,updateDate, updatedBy)]
  ## Create a dataframe from the data
  silver_new_record_df = spark.createDataFrame(data, schema_definition)
  
  ## Get the DeltaTable object for the Silver Metadata table
  silver_md_table = DeltaTable.forName(spark,SILVER_MD_TABLE)

  ## Merge the new record with the existing records in the Silver Metadata table
  silver_md_table.alias('silver_md') \
  .merge(silver_new_record_df.alias('updates'),'silver_md.dataFlowId = updates.dataFlowId and silver_md.dataFlowGroup = updates.dataFlowGroup') \
  .whenMatchedUpdate(set =
    {
      "dataFlowId": "updates.dataFlowId",
      "dataFlowGroup": "updates.dataFlowGroup",
      "sourceFormat": "updates.sourceFormat",
      "sourceDetails": "updates.sourceDetails",
      "readerConfigOptions": "updates.readerConfigOptions",
      "targetFormat": "updates.targetFormat",
      "targetDetails":"updates.targetDetails",
      "tableProperties":"updates.tableProperties",
      "selectExp":"updates.selectExp",
      "whereClause":"updates.whereClause",
      "partitionColumns":"updates.partitionColumns",
      "cdcApplyChanges":"updates.cdcApplyChanges",
      "dataQualityExpectations":"updates.dataQualityExpectations",
      "version": lit(get_next_version(SILVER_MD_TABLE,dataFlowId,dataFlowGroup,spark)),
      "createDate":"silver_md.createDate",
      "createdBy":"silver_md.createdBy",
      "updateDate":"updates.updateDate",
      "updatedBy":"updates.updatedBy"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "dataFlowId": "updates.dataFlowId",
      "dataFlowGroup": "updates.dataFlowGroup",
      "sourceFormat": "updates.sourceFormat",
      "sourceDetails": "updates.sourceDetails",
      "readerConfigOptions": "updates.readerConfigOptions",
      "targetFormat": "updates.targetFormat",
      "targetDetails":"updates.targetDetails",
      "tableProperties":"updates.tableProperties",
      "selectExp":"updates.selectExp",
      "whereClause":"updates.whereClause",
      "partitionColumns":"updates.partitionColumns",
      "cdcApplyChanges":"updates.cdcApplyChanges",
      "dataQualityExpectations":"updates.dataQualityExpectations",
      "version": lit(get_next_version(SILVER_MD_TABLE,dataFlowId,dataFlowGroup,spark)),
      "createDate":"updates.createDate",
      "createdBy":"updates.createdBy",
      "updateDate":"updates.updateDate",
      "updatedBy":"updates.updatedBy"
    }
  ) \
  .execute()