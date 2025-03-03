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
    partitionColumns ARRAY < STRING >, \
    cdcApplyChanges STRING, \
    materializedView STRING, \
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )