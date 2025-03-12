"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame # pylint: disable=import-error # type: ignore
from pyspark.sql.types import StructType # pylint: disable=import-error # type: ignore
from pyspark.sql.functions import from_json, col, current_timestamp # pylint: disable=import-error # type: ignore


logger = logging.getLogger('databricks.logger')
logger.setLevel(logging.INFO)


class PipelineReaders:
    """PipelineReader Class.

    Returns:
        _type_: _description_
    """

    @staticmethod
    def read_dlt_cloud_files(spark, bronze_dataflow_spec, schema_json) -> DataFrame:
        """Read dlt cloud files.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        source_path = bronze_dataflow_spec.sourceDetails["path"]
        reader_config_options = bronze_dataflow_spec.readerConfigOptions
        
        ## If the cloudFiles.useNotifications is set to True, then read the secrets from the KeyVault
        ## and set the secrets to the reader_config_options
        ## Also remove the extra configs from the reader_config_options
        if "cloudFiles.useNotifications" in reader_config_options and reader_config_options["cloudFiles.useNotifications"] == "true":
            dbutils = PipelineReaders.get_db_utils(spark)
            cloudFileNotificationsConfig = bronze_dataflow_spec.cloudFileNotificationsConfig

            ## FROM JSON CONFIG FILE READ THE CONFIGS
            config_file = dbutils.fs.head(cloudFileNotificationsConfig.get("configJsonFilePath"))
            config = json.loads(config_file)
        
            ## SET THE SECRETS TO THE READER CONFIG OPTIONS
            reader_config_options["cloudFiles.subscriptionId"] = config[cloudFileNotificationsConfig.get("cloudFiles.subscriptionId")]
            reader_config_options["cloudFiles.resourceGroup"] = config[cloudFileNotificationsConfig.get("cloudFiles.resourceGroup")]
            
            reader_config_options["cloudFiles.clientId"] = dbutils.secrets.get(
                config[cloudFileNotificationsConfig.get("kvSecretScope")] 
                ,config[cloudFileNotificationsConfig.get("cloudFiles.clientId")])
            
            reader_config_options["cloudFiles.clientSecret"] = dbutils.secrets.get(
                config[cloudFileNotificationsConfig.get("kvSecretScope")] 
                ,config[cloudFileNotificationsConfig.get("cloudFiles.clientSecret")])
            
            reader_config_options["cloudFiles.tenantId"] = dbutils.secrets.get(
                config[cloudFileNotificationsConfig.get("kvSecretScope")] 
                ,config[cloudFileNotificationsConfig.get("cloudFiles.tenantId")])
            

        if schema_json and bronze_dataflow_spec.sourceFormat.lower() != "delta":
            schema = StructType.fromJson(schema_json)
            return (
                spark.readStream.format(bronze_dataflow_spec.sourceFormat)
                .options(**reader_config_options)
                .schema(schema)
                .load(source_path)
                .select("*","_metadata.file_path",current_timestamp().alias("processing_time"))
            )
        else:
            return (
                spark.readStream.format(bronze_dataflow_spec.sourceFormat)
                .options(**reader_config_options)
                .load(source_path)
                .select("*","_metadata.file_path",current_timestamp().alias("processing_time"))
            )

    @staticmethod
    def read_dlt_delta(spark, bronze_dataflow_spec) -> DataFrame:
        """Read dlt delta.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        reader_config_options = bronze_dataflow_spec.readerConfigOptions

        if reader_config_options and len(reader_config_options) > 0:
            return (
                spark.readStream.options(**reader_config_options).table(
                    f"""{bronze_dataflow_spec.sourceDetails["source_database"]}
                        .{bronze_dataflow_spec.sourceDetails["table"]}"""
                )
            )
        else:
            return (
                spark.readStream.table(
                    f"""{bronze_dataflow_spec.sourceDetails["source_database"]}
                        .{bronze_dataflow_spec.sourceDetails["table"]}"""
                )
            )

    @staticmethod
    def get_db_utils(spark):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils # pylint: disable=import-error disable=import-outside-toplevel # type: ignore
        return DBUtils(spark)

    @staticmethod
    def read_kafka(spark, bronze_dataflow_spec, schema_json) -> DataFrame:
        """Read eventhub with dataflowspec and schema.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        if bronze_dataflow_spec.sourceFormat == "eventhub":
            kafka_options = PipelineReaders.get_eventhub_kafka_options(spark, bronze_dataflow_spec)
        elif bronze_dataflow_spec.sourceFormat == "kafka":
            kafka_options = PipelineReaders.get_kafka_options(spark, bronze_dataflow_spec)
        raw_df = (
            spark
            .readStream
            .format("kafka")
            .options(**kafka_options) # pylint: disable=possibly-used-before-assignment
            .load()
            # add date, hour, and minute columns derived from eventhub enqueued timestamp
            .selectExpr("*", "to_date(timestamp) as date", "hour(timestamp) as hour", "minute(timestamp) as minute")
        )
        if schema_json:
            schema = StructType.fromJson(schema_json)
            return (
                raw_df.withColumn("parsed_records", from_json(col("value").cast("string"), schema))
            )
        else:
            return raw_df

    @staticmethod
    def get_eventhub_kafka_options(spark, bronze_dataflow_spec):
        """Get eventhub options from dataflowspec."""
        dbutils = PipelineReaders.get_db_utils(spark)
        eh_namespace = bronze_dataflow_spec.sourceDetails.get("eventhub.namespace")
        eh_port = bronze_dataflow_spec.sourceDetails.get("eventhub.port")
        eh_name = bronze_dataflow_spec.sourceDetails.get("eventhub.name")
        eh_shared_key_name = bronze_dataflow_spec.sourceDetails.get("eventhub.accessKeyName")
        secret_name = bronze_dataflow_spec.sourceDetails.get("eventhub.accessKeySecretName")
        if not secret_name:
            # set default value if "eventhub.accessKeySecretName" is not specified
            secret_name = eh_shared_key_name
        secret_scope = bronze_dataflow_spec.sourceDetails.get("eventhub.secretsScopeName")
        eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
        eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
        eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
        eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

        eh_conn_options = {
            "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
            "subscribe": eh_name,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": sasl_config
        }
        kafka_options = {**eh_conn_options, **bronze_dataflow_spec.readerConfigOptions}
        return kafka_options

    @staticmethod
    def get_kafka_options(spark, bronze_dataflow_spec):
        """Get kafka options from dataflowspec."""
        source_details_map = bronze_dataflow_spec.sourceDetails
        kafka_base_ops = {
            "kafka.bootstrap.servers": source_details_map.get("kafka.bootstrap.servers"),
            "subscribe": source_details_map.get("subscribe")
        }
        ssl_truststore_location = source_details_map.get("kafka.ssl.truststore.location", None)
        ssl_keystore_location = source_details_map.get("kafka.ssl.keystore.location", None)
        if ssl_truststore_location and ssl_keystore_location:
            truststore_scope = source_details_map.get("kafka.ssl.truststore.secrets.scope", None)
            truststore_key = source_details_map.get("kafka.ssl.truststore.secrets.key", None)
            keystore_scope = source_details_map.get("kafka.ssl.keystore.secrets.scope", None)
            keystore_key = source_details_map.get("kafka.ssl.keystore.secrets.key", None)
            if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                dbutils = PipelineReaders.get_db_utils(spark)
                kafka_ssl_conn = {
                    "kafka.ssl.truststore.location": ssl_truststore_location,
                    "kafka.ssl.keystore.location": ssl_keystore_location,
                    "kafka.ssl.keystore.password": dbutils.secrets.get(keystore_scope, keystore_key),
                    "kafka.ssl.truststore.password": dbutils.secrets.get(truststore_scope, truststore_key)
                }
                kafka_options = {**kafka_base_ops, **kafka_ssl_conn, **bronze_dataflow_spec.readerConfigOptions}
            else:
                params = ["kafka.ssl.truststore.secrets.scope",
                          "kafka.ssl.truststore.secrets.key",
                          "kafka.ssl.keystore.secrets.scope",
                          "kafka.ssl.keystore.secrets.key"
                          ]
                raise ValueError(f"Kafka ssl required params are: {params}! provided options are :{source_details_map}")
        else:
            kafka_options = {**kafka_base_ops, **bronze_dataflow_spec.readerConfigOptions}
        return kafka_options