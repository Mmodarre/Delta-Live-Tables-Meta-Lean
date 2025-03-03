# Databricks notebook source
dbutils.widgets.text('Metadata_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')

# COMMAND ----------

dbutils.widgets.text('checkpoint_volume',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Table',defaultValue='bronze_dataflowspec_table')

# COMMAND ----------

dbutils.widgets.text('intergration_logs_table',defaultValue='data_intergration_logs')

# COMMAND ----------

dbutils.widgets.text('dataFlowGroup',defaultValue='')

# COMMAND ----------

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

# COMMAND ----------

meta_schema = dbutils.widgets.get("Metadata_Schema")

# COMMAND ----------

checkpoint_volume = dbutils.widgets.get("checkpoint_volume")

# COMMAND ----------

meta_table = dbutils.widgets.get("Metadata_Table")

# COMMAND ----------

dataFlowGroup = dbutils.widgets.get("dataFlowGroup")

# COMMAND ----------

integration_logs_table = dbutils.widgets.get("intergration_logs_table")

# COMMAND ----------

# DBTITLE 1,## Streaming Data Integration Logs Upsert Logic Using Bronze Table Change Data Feed
from pyspark.sql.functions import col, expr, window

# metadata_table = f"{meta_catalog}.{meta_schema}.{meta_table}"
# integration_table = f"{meta_catalog}.{meta_schema}.{integration_logs_table}"


def upsertToDelta(microBatchOutputDF, batchId):
    microBatchOutputDF.createOrReplaceTempView("updates")
    microBatchOutputDF.sparkSession.sql(
        f"""
    MERGE INTO {meta_catalog}.{meta_schema}.{integration_logs_table} t
    USING updates s
    ON s.contract_id = t.contract_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """
    )


df = spark.read.table(f"{meta_catalog}.{meta_schema}.{meta_table}").where(
    col("dataFlowGroup") == dataFlowGroup
)
targets = df.select("targetDetails", "sourceFormat", "highWaterMark")
for row in targets.collect():
    catalog = row["targetDetails"]["database"].split(".")[0]
    schema = row["targetDetails"]["database"].split(".")[1]
    table = row["targetDetails"]["table"]
    full_target_table = f"{catalog}.{schema}.{table}"
    contract_id = row["highWaterMark"]["contract_id"]
    contract_version = row["highWaterMark"]["contract_version"]
    contract_major_version = row["highWaterMark"]["contract_major_version"]
    watermark_column = row["highWaterMark"]["watermark_column"]

    result = (
        spark.readStream.option("readChangeFeed", "true")
        .format("delta")
        .table(
            full_target_table
        )
        .groupBy()
        .agg(
            expr(f"'{contract_id}' as contract_id"),
            expr(f"'{contract_version}' as contract_version"),
            expr(f"'{contract_major_version}' as contract_major_version"),
            expr(f"""concat("([{watermark_column}] > '",cast(max({watermark_column}) as string), "')") as watermark_next_value"""), ##([dtEvent] > '2024-12-23 11:59:25.713000')
            expr(f"'{full_target_table}' as target_table"),
            expr(f"max(file_path) as source_file"),
            expr(f"current_timestamp() as __insert_ts")
        )
    )

    #display(result)

    (
        result.writeStream.foreachBatch(upsertToDelta)
        .outputMode("update")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"/Volumes/{meta_catalog}/{meta_schema}/{checkpoint_volume}/checkpoints/data_integration_logs/contract_id/{contract_id}",
        )
        .start()
    )
