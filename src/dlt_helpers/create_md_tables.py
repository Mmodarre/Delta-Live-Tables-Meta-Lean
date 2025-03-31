# Databricks notebook source
dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')

# COMMAND ----------

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

# COMMAND ----------

meta_schema = dbutils.widgets.get("Metadata_Schema")

# COMMAND ----------

env =dbutils.widgets.get("env")

# COMMAND ----------

# DBTITLE 1,Create Bronze Metadata Table

#pylint: disable=undefined-variable
spark.sql(f"CREATE TABLE IF NOT EXISTS {meta_catalog}{env}.{meta_schema}.bronze_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < STRING,STRING >, \
    highWaterMark MAP < STRING, STRING >, \
    readerConfigOptions MAP < STRING, STRING >, \
    cloudFileNotificationsConfig MAP < STRING,STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING, STRING >, \
    tableProperties MAP < STRING, STRING >, \
    schema STRING, \
    partitionColumns ARRAY < STRING >,\
    liquidClusteringColumns ARRAY < STRING >,\
    cdcApplyChanges STRING, \
    dataQualityExpectations STRING,\
    quarantineTargetDetails MAP < STRING, STRING >, \
    quarantineTableProperties MAP < STRING,STRING >, \
    version STRING, \
    createDate TIMESTAMP,\
    createdBy STRING,\
    updateDate TIMESTAMP,\
    updatedBy STRING)"
    )




# COMMAND ----------

# DBTITLE 1,Create Silver Metadata Table
spark.sql(f'CREATE TABLE IF NOT EXISTS {meta_catalog}{env}.{meta_schema}.silver_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < STRING, STRING >, \
    readerConfigOptions MAP < STRING, STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING,STRING >, \
    tableProperties MAP < STRING,STRING >, \
    selectExp ARRAY < STRING >, \
    whereClause ARRAY < STRING >, \
    partitionColumns ARRAY < STRING >,\
    liquidClusteringColumns ARRAY < STRING >,\
    cdcApplyChanges STRING, \
    materializedView STRING, \
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )

# COMMAND ----------

# DBTITLE 1,Create Data Integration Logs Table to hold High Watermarks
spark.sql(f"""CREATE TABLE IF NOT EXISTS {meta_catalog}{env}.{meta_schema}.data_integration_logs (
  contract_id STRING NOT NULL,
  contract_version DECIMAL(7,3) NOT NULL DEFAULT 1.0,
  contract_major_version INT NOT NULL DEFAULT 1,
  watermark_next_value STRING,
  target_table STRING,
  source_file STRING,
  __insert_ts TIMESTAMP NOT NULL DEFAULT current_timestamp())
  TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.changeDataFeed' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.generatedColumns' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7') """)
