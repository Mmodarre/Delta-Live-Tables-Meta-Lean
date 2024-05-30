from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    ArrayType,
    TimestampType
)
import datetime
from pyspark.sql.functions import current_user

def populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,version,createDate,createdBy,updateDate,updatedBy):

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
    StructField('version', StringType(), True),
    StructField('createDate', TimestampType(), True),
    StructField('createdBy', StringType(), True),
    StructField('updateDate', TimestampType(), True),
    StructField('updatedBy', StringType(), True)
])
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions,cloudFileNotificationsConfig, schema, targetFormat, targetDetails, tableProperties,partitionColumns, cdcApplyChanges, dataQualityExpectations, quarantineTargetDetails,quarantineTableProperties, version,createDate, createdBy,updateDate, updatedBy)]

  df = spark.createDataFrame(data, schema_definition)
  df.write.format("delta").mode("append").saveAsTable(BRONZE_MD_TABLE)


def populate_silver(SILVER_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,targetFormat,targetDetails,tableProperties,selectExp,whereClause,partitionColumns,cdcApplyChanges,dataQualityExpectations,version,createDate,createdBy,updateDate,updatedBy):

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
            StructField("version", StringType(), True),
            StructField("createDate", TimestampType(), True),
            StructField("createdBy", StringType(), True),
            StructField("updateDate", TimestampType(), True),
            StructField("updatedBy", StringType(), True),
        ]
    )
  
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,
         selectExp,whereClause,partitionColumns, cdcApplyChanges, dataQualityExpectations,
         version,createDate, createdBy,updateDate, updatedBy)]

  df = spark.createDataFrame(data, schema_definition)
  df.write.format("delta").mode("append").saveAsTable(SILVER_MD_TABLE)