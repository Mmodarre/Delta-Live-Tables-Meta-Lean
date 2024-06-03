# Databricks notebook source
dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

spark.sql('CREATE SCHEMA IF NOT EXISTS mehdidatalake_catalog'+dbutils.widgets.get('env')+'._meta')

# COMMAND ----------


spark.sql('CREATE TABLE IF NOT EXISTS mehdidatalake_catalog'+dbutils.widgets.get('env')+'._meta.bronze_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < STRING,STRING >, \
    readerConfigOptions MAP < STRING, STRING >, \
    cloudFileNotificationsConfig MAP < STRING,STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING, STRING >, \
    tableProperties MAP < STRING, STRING >, \
    schema STRING, \
    partitionColumns ARRAY < STRING >,\
    cdcApplyChanges STRING, \
    dataQualityExpectations STRING,\
    quarantineTargetDetails MAP < STRING, STRING >, \
    quarantineTableProperties MAP < STRING,STRING >, \
    version STRING, \
    createDate TIMESTAMP,\
    createdBy STRING,\
    updateDate TIMESTAMP,\
    updatedBy STRING)'
    )


spark.sql('CREATE TABLE IF NOT EXISTS mehdidatalake_catalog'+dbutils.widgets.get('env')+'._meta.silver_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < ST RING, \
    STRING >, \
    readerConfigOptions MAP < STRING, \
    STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING, \
    STRING >, \
    tableProperties MAP < STRING, \
    STRING >, \
    selectExp ARRAY < STRING >, \
    whereClause ARRAY < STRING >, \
    partitionColumns ARRAY < STRING >, \
    cdcApplyChanges STRING, \
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )