# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## NOTEBOOK PURPOSE:
# MAGIC - This notebook tracks high watermark values from target tables and updates a central logging table
# MAGIC - High watermarks are used to track data processing progress and enable incremental loading
# MAGIC - Set up input parameters through widgets
# MAGIC
# MAGIC ##### This Notebook Assumes `data_intergation_logs` table is created

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating parameters using Notebook widgets

# COMMAND ----------

dbutils.widgets.text('Metadata_Catalog',defaultValue='')  # Catalog containing metadata tables

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')  # Schema for metadata tables

# COMMAND ----------

dbutils.widgets.text('checkpoint_volume',defaultValue='')  # Volume for streaming checkpoints

# COMMAND ----------

dbutils.widgets.text('Metadata_Table',defaultValue='bronze_dataflowspec_table')  # Table containing dataflow configurations

# COMMAND ----------

dbutils.widgets.text('integration_logs_table',defaultValue='data_intergration_logs')  # Target table for logging watermarks

# COMMAND ----------

dbutils.widgets.text('dataFlowGroup',defaultValue='')  # Filter for specific dataflow group

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get parameter values from widgets

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

integration_logs_table = dbutils.widgets.get("integration_logs_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Function to merge streaming watermark using the Bronze Table Change Data Feed to Integration Logs Table

# COMMAND ----------

# DBTITLE 1,## Streaming Data Integration Logs Upsert Logic Using Bronze Table Change Data Feed
from pyspark.sql.functions import col, expr


def upsertToDelta(microBatchOutputDF, batchId):
    """
    Function to merge streaming watermark updates into the integration logs table.

    Args:
        microBatchOutputDF: DataFrame containing the batch of new watermark values
        batchId: ID of the current microbatch
    """
    microBatchOutputDF.createOrReplaceTempView("updates")
    microBatchOutputDF.sparkSession.sql(
        f"""
        MERGE INTO {meta_catalog}.{meta_schema}.{integration_logs_table} t
        USING updates s
        ON s.contract_id = t.contract_id
        WHEN MATCHED 
        AND md5(CONCAT(
            coalesce(CAST(s.watermark_next_value AS STRING),""),
            coalesce(CAST(s.source_file AS STRING),"")
            )) != 
            md5(CONCAT(
                coalesce(CAST(t.watermark_next_value AS STRING),""),
                coalesce(CAST(t.source_file AS STRING),"")
                ))
                THEN UPDATE SET 
                watermark_next_value = s.watermark_next_value,
                source_file = s.source_file,
                `__insert_ts` = current_timestamp()

        WHEN NOT MATCHED THEN 
        INSERT 
        (contract_id,
        contract_version,
        contract_major_version,
        watermark_next_value,
        target_table,
        source_file,
        `__insert_ts`) 
        VALUES (s.contract_id,
        s.contract_version,
        s.contract_major_version,
        s.watermark_next_value,
        s.target_table,
        s.source_file,
        current_timestamp())
        """
    )


# Read the metadata table filtered by dataflow group
df = spark.read.table(f"{meta_catalog}.{meta_schema}.{meta_table}").where(
    (col("dataFlowGroup") == dataFlowGroup) & (col("highWaterMark").isNotNull())
)

# Extract target details and high watermark configuration
targets = df.select("targetDetails", "sourceFormat", "highWaterMark")

# Process each target table to track its watermark
for row in targets.collect():
    # Extract target table information
    catalog = row["targetDetails"]["database"].split(".")[0]
    schema = row["targetDetails"]["database"].split(".")[1]
    table = row["targetDetails"]["table"]
    full_target_table = f"{catalog}.{schema}.{table}"

    if (
        spark.sql(
            f"""
                 select count(*) from {full_target_table}
                 """
        ).collect()[0][0]
        == 0
    ):
        continue

    # Extract high watermark tracking information
    contract_id = row["highWaterMark"]["contract_id"]
    contract_version = row["highWaterMark"]["contract_version"]
    contract_major_version = row["highWaterMark"]["contract_major_version"]
    watermark_column = row["highWaterMark"]["watermark_column"]

    # Create a streaming query to track the max watermark value
    # Uses Delta Change Data Feed to capture changes efficiently
    result = (
        spark.readStream.option("readChangeFeed", "true")
        .format("delta")
        .table(full_target_table)
        .groupBy()
        .agg(
            # Create tracking columns for the watermark log
            expr(f"'{contract_id}' as contract_id"),
            expr(f"'{contract_version}' as contract_version"),
            expr(f"'{contract_major_version}' as contract_major_version"),
            # Format the watermark as a SQL expression for future use (e.g., "[dtEvent] > '2024-12-23 11:59:25.713000'")
            expr(
                f"""concat("([{watermark_column}] > '",cast(max({watermark_column}) as string), "')") as watermark_next_value"""
            ),
            expr(f"'{full_target_table}' as target_table"),
            expr("max(file_path) as source_file"),
        )
    )

    # Write the watermark tracking information to the integration logs table
    # Uses foreachBatch to apply the upsert logic
    (
        result.writeStream.foreachBatch(upsertToDelta)
        .outputMode("update")
        .trigger(availableNow=True)  # Process available data and stop
        .option(
            "checkpointLocation",
            f"/Volumes/{meta_catalog}/{meta_schema}/{checkpoint_volume}/checkpoints/data_integration_logs/contract_id/{contract_id}",
        )
        .start()
    )