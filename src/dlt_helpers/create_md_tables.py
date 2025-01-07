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
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------



spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}{env}.{schema}.bronze_dataflowspec_table ( \
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
    updatedBy STRING)"
    )




# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS {catalog}{env}.{schema}.silver_dataflowspec_table ( \
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
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )