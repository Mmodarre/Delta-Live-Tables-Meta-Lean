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

def get_next_version(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,spark):
  current_version = spark.sql('select version from '+BRONZE_MD_TABLE+' where dataFlowId="'+dataFlowId+'" and dataFlowGroup = "'+dataFlowGroup+'"').collect()
  if len(current_version)  >0:
    next_version = current_version[0]['version']
    return "v"+str(int(next_version[1:])+1)
  else:
    return "v1"

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

  bronze_new_record_df = spark.createDataFrame(data, schema_definition)
  bronze_md_table = DeltaTable.forName(spark,BRONZE_MD_TABLE)

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

  silver_new_record_df = spark.createDataFrame(data, schema_definition)
  silver_md_table = DeltaTable.forName(spark,SILVER_MD_TABLE)

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