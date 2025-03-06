from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    ArrayType,
    TimestampType
)
from pyspark.sql.functions import col, lit, md5, concat, coalesce

## Function to populate the Bronze Metadata table
##
## @param BRONZE_MD_TABLE: The name of the Bronze Metadata table
## @param dataFlowId: The unique identifier for the pipeline
## @param dataFlowGroup: The group to which the pipeline belongs
## @param sourceFormat: The format of the source data
## @param sourceDetails: The details of the source data
## @param highWaterMark: The high water mark for the pipeline
## @param readerConfigOptions: The configuration options for the reader
## @param cloudFileNotificationsConfig: The configuration options for cloud file notifications
## @param schema: The schema of the source data
## @param targetFormat: The format of the target data
## @param targetDetails: The details of the target data
## @param tableProperties: The properties of the target table
## @param partitionColumns: The partition columns for the target table
## @param liquidClusteringColumns: The Liquid Clustering columns for the target table
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
def populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,highWaterMark,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,liquidClusteringColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark):

  schema_definition = StructType([
  StructField('dataFlowId', StringType(), True),
  StructField('dataFlowGroup', StringType(), True),
  StructField('sourceFormat', StringType(), True),
  StructField('sourceDetails', MapType(StringType(), StringType(), True), True),
  StructField('highWaterMark', MapType(StringType(), StringType(), True), True),
  StructField('readerConfigOptions', MapType(StringType(), StringType(), True), True),
  StructField('cloudFileNotificationsConfig', MapType(StringType(), StringType(), True), True),
  StructField('schema',StringType(),True),
  StructField('targetFormat', StringType(), True),
  StructField('targetDetails', MapType(StringType(), StringType(), True), True),
  StructField('tableProperties', MapType(StringType(), StringType(), True), True),
  StructField('partitionColumns', ArrayType(StringType(), True), True),
  StructField('liquidClusteringColumns', ArrayType(StringType(), True), True),
  StructField('cdcApplyChanges', StringType(), True),
  StructField('dataQualityExpectations', StringType(), True),
  StructField('quarantineTargetDetails', MapType(StringType(), StringType(), True), True),
  StructField('quarantineTableProperties', MapType(StringType(), StringType(), True), True),
  StructField('createDate', TimestampType(), True), ## NEED TO REMOVE
  StructField('createdBy', StringType(), True), ## NEED TO REMOVE
  StructField('updateDate', TimestampType(), True), ## NEED TO REMOVE
  StructField('updatedBy', StringType(), True) ## NEED TO REMOVE
])
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails,highWaterMark, readerConfigOptions,cloudFileNotificationsConfig, schema, targetFormat, targetDetails, tableProperties,partitionColumns,liquidClusteringColumns, cdcApplyChanges, dataQualityExpectations, quarantineTargetDetails,quarantineTableProperties,createDate, createdBy,updateDate, updatedBy)]
  ## Create a dataframe from the data
  bronze_new_record_df = spark.createDataFrame(data, schema_definition) 
  bronze_new_record_df = spark.createDataFrame(data, schema_definition)
  bronze_new_record_df = bronze_new_record_df.withColumn("hash", md5(concat(*[coalesce(col(c).cast("string"),lit("")) for c in bronze_new_record_df.columns if c not in ["dataFlowId","dataFlowGroup","createDate","createdBy","updateDate","updatedBy","version"]])))
  ## Create a temporary view for the new record
  bronze_new_record_df.createOrReplaceTempView("bronze_new_record")

  spark.sql(f"""
  MERGE INTO {BRONZE_MD_TABLE} as bronze_md
USING bronze_new_record as updates
ON bronze_md.dataflowId = updates.dataflowId AND bronze_md.dataflowGroup = updates.dataflowGroup 
WHEN MATCHED and 
md5(CONCAT(
  coalesce(CAST(bronze_md.sourceFormat AS STRING),""),
  coalesce(CAST(bronze_md.sourceDetails AS STRING),""),
  coalesce(CAST(bronze_md.highWaterMark AS STRING),""),
  coalesce(CAST(bronze_md.readerConfigOptions AS STRING),""),
  coalesce(CAST(bronze_md.cloudFileNotificationsConfig AS STRING),""),
  coalesce(CAST(bronze_md.schema AS STRING),""),
  coalesce(CAST(bronze_md.targetFormat AS STRING),""),
  coalesce(CAST(bronze_md.targetDetails AS STRING),""),
  coalesce(CAST(bronze_md.tableProperties AS STRING),""),
  coalesce(CAST(bronze_md.partitionColumns AS STRING),""),
  coalesce(CAST(bronze_md.liquidClusteringColumns AS STRING),""),
  coalesce(CAST(bronze_md.cdcApplyChanges AS STRING),""),
  coalesce(CAST(bronze_md.dataQualityExpectations AS STRING),""),
  coalesce(CAST(bronze_md.quarantineTargetDetails AS STRING),""),
  coalesce(CAST(bronze_md.quarantineTableProperties AS STRING),"")
)) != updates.hash THEN
  UPDATE SET
    dataFlowId = updates.dataFlowId,
    dataFlowGroup = updates.dataFlowGroup,
    sourceFormat = updates.sourceFormat,
    sourceDetails = updates.sourceDetails,
    highWaterMark = updates.highWaterMark,
    readerConfigOptions = updates.readerConfigOptions,
    cloudFileNotificationsConfig = updates.cloudFileNotificationsConfig,
    schema = updates.schema,
    targetFormat = updates.targetFormat,
    targetDetails = updates.targetDetails,
    tableProperties = updates.tableProperties,
    partitionColumns = updates.partitionColumns,
    liquidClusteringColumns = updates.liquidClusteringColumns,
    cdcApplyChanges = updates.cdcApplyChanges,
    dataQualityExpectations = updates.dataQualityExpectations,
    quarantineTargetDetails = updates.quarantineTargetDetails,
    quarantineTableProperties = updates.quarantineTableProperties,
    version = CONCAT('v',CAST(CAST(substr(bronze_md.version, 2) AS INTEGER)+1 AS STRING)),
    createDate = bronze_md.createDate,
    createdBy = bronze_md.createdBy,
    updateDate = updates.updateDate,
    updatedBy = updates.updatedBy
WHEN NOT MATCHED
  THEN INSERT (
    dataFlowId,
    dataFlowGroup,
    sourceFormat,
    sourceDetails,
    highWaterMark,
    readerConfigOptions,
    cloudFileNotificationsConfig,
    schema,
    targetFormat,
    targetDetails,
    tableProperties,
    partitionColumns,
    liquidClusteringColumns,
    cdcApplyChanges,
    dataQualityExpectations,
    quarantineTargetDetails,
    quarantineTableProperties,
    version,
    createDate,
    createdBy,
    updateDate,
    updatedBy
  )
  VALUES (
    updates.dataFlowId,
    updates.dataFlowGroup,
    updates.sourceFormat,
    updates.sourceDetails,
    updates.highWaterMark,
    updates.readerConfigOptions,
    updates.cloudFileNotificationsConfig,
    updates.schema,
    updates.targetFormat,
    updates.targetDetails,
    updates.tableProperties,
    updates.partitionColumns,
    updates.liquidClusteringColumns,
    updates.cdcApplyChanges,
    updates.dataQualityExpectations,
    updates.quarantineTargetDetails,
    updates.quarantineTableProperties,
    'v1',
    updates.createDate,
    updates.createdBy,
    updates.updateDate,
    updates.updatedBy
  )
""")



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
## @param liquidClusteringColumns: The Liquid Clustering columns for the target table
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

def populate_silver(SILVER_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,targetFormat,targetDetails,tableProperties,selectExp,whereClause,partitionColumns,liquidClusteringColumns,cdcApplyChanges,materializedView,dataQualityExpectations,createDate,createdBy,updateDate,updatedBy,spark):

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
            StructField("liquidClusteringColumns", ArrayType(StringType(), True), True),
            StructField("cdcApplyChanges", StringType(), True),
            StructField("materializedView", StringType(), True),
            StructField("dataQualityExpectations", StringType(), True),
            StructField("createDate", TimestampType(), True), ## NEED TO REMOVE
            StructField("createdBy", StringType(), True), ## NEED TO REMOVE
            StructField("updateDate", TimestampType(), True), ## NEED TO REMOVE
            StructField("updatedBy", StringType(), True), ## NEED TO REMOVE
        ]
    )
  data = [(dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,
         selectExp,whereClause,partitionColumns,liquidClusteringColumns,cdcApplyChanges,materializedView, dataQualityExpectations
         ,createDate, createdBy,updateDate, updatedBy)]
  ## Create a dataframe from the data
  silver_new_record_df = spark.createDataFrame(data, schema_definition)
  silver_new_record_df = silver_new_record_df.withColumn("hash", md5(concat(*[coalesce(col(c).cast("string"),lit("")) for c in silver_new_record_df.columns if c not in ["dataFlowId","dataFlowGroup","createDate","createdBy","updateDate","updatedBy","version"]])))
  ## Create a temporary view for the new record
  silver_new_record_df.createOrReplaceTempView("silver_new_record")

  ## Merge the new record with the existing records in the Silver Metadata table
  spark.sql(f"""
  MERGE INTO {SILVER_MD_TABLE} as silver_md
USING silver_new_record as updates
ON silver_md.dataflowId = updates.dataflowId AND silver_md.dataflowGroup = updates.dataflowGroup 
WHEN MATCHED and 
md5(CONCAT(
  coalesce(CAST(silver_md.sourceFormat AS STRING),""),
  coalesce(CAST(silver_md.sourceDetails AS STRING),""),
  coalesce(CAST(silver_md.readerConfigOptions AS STRING),""),
  coalesce(CAST(silver_md.targetFormat AS STRING),""),
  coalesce(CAST(silver_md.targetDetails AS STRING),""),
  coalesce(CAST(silver_md.tableProperties AS STRING),""),
  coalesce(CAST(silver_md.selectExp AS STRING),""),
  coalesce(CAST(silver_md.whereClause AS STRING),""),
  coalesce(CAST(silver_md.partitionColumns AS STRING),""),
  coalesce(CAST(silver_md.liquidClusteringColumns AS STRING),""),
  coalesce(CAST(silver_md.cdcApplyChanges AS STRING),""),
  coalesce(CAST(silver_md.materializedView AS STRING),""),
  coalesce(CAST(silver_md.dataQualityExpectations AS STRING),"")
)) != updates.hash THEN
  UPDATE SET
    dataFlowId = updates.dataFlowId,
    dataFlowGroup = updates.dataFlowGroup,
    sourceFormat = updates.sourceFormat,
    sourceDetails = updates.sourceDetails,
    readerConfigOptions = updates.readerConfigOptions,
    targetFormat = updates.targetFormat,
    targetDetails = updates.targetDetails,
    tableProperties = updates.tableProperties,
    selectExp = updates.selectExp,
    whereClause = updates.whereClause,
    partitionColumns = updates.partitionColumns,
    liquidClusteringColumns = updates.liquidClusteringColumns,
    cdcApplyChanges = updates.cdcApplyChanges,
    materializedView = updates.materializedView,
    dataQualityExpectations = updates.dataQualityExpectations,
    version = CONCAT('v',CAST(CAST(substr(silver_md.version, 2) AS INTEGER)+1 AS STRING)),
    createDate = silver_md.createDate,
    createdBy = silver_md.createdBy,
    updateDate = updates.updateDate,
    updatedBy = updates.updatedBy
WHEN NOT MATCHED
  THEN INSERT (
    dataFlowId,
    dataFlowGroup,
    sourceFormat,
    sourceDetails,
    readerConfigOptions,
    targetFormat,
    targetDetails,
    tableProperties,
    selectExp,
    whereClause,
    partitionColumns,
    liquidClusteringColumns,
    cdcApplyChanges,
    materializedView,
    dataQualityExpectations,
    version,
    createDate,
    createdBy,
    updateDate,
    updatedBy
  )
  VALUES (
    updates.dataFlowId,
    updates.dataFlowGroup,
    updates.sourceFormat,
    updates.sourceDetails,
    updates.readerConfigOptions,
    updates.targetFormat,
    updates.targetDetails,
    updates.tableProperties,
    updates.selectExp,
    updates.whereClause,
    updates.partitionColumns,
    updates.liquidClusteringColumns,
    updates.cdcApplyChanges,
    updates.materializedView,
    updates.dataQualityExpectations,
    'v1',
    updates.createDate,
    updates.createdBy,
    updates.updateDate,
    updates.updatedBy
  )
""")
