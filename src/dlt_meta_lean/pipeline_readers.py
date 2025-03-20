"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame # pylint: disable=import-error # type: ignore
from pyspark.sql.types import StructType # pylint: disable=import-error # type: ignore
from pyspark.sql.functions import from_json, col, current_timestamp # pylint: disable=import-error # type: ignore


logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class PipelineReaders:
    """PipelineReader Class.

    Returns:
        _type_: _description_
    """
    def __init__(self, spark, source_format, source_details, reader_config_options,cloud_file_notifications_config, schema_json=None):
        """Init."""
        self.spark = spark
        self.source_format = source_format
        self.source_details = source_details
        self.reader_config_options = reader_config_options
        self.schema_json = schema_json
        self.cloud_file_notifications_config = cloud_file_notifications_config

    @staticmethod
    def set_cloud_file_notification_config(spark, reader_config_options, cloud_file_notifications_config):
        """
            - If the cloudFiles.useNotifications is set to True,
              then read the secrets from the KeyVault
              and set the secrets to the reader_config_options.
            - Also remove the extra configs from the reader_config_options

        Args:
            spark: Spark session
            reader_config_options (dict): Reader configuration options
            cloudFileNotificationsConfig (dict): Cloud file notification configuration

        Returns:
            dict: Updated reader configuration options
        """
        dbutils = PipelineReaders.get_db_utils(spark)

        ## FROM JSON CONFIG FILE READ THE CONFIGS
        config_file = dbutils.fs.head(cloud_file_notifications_config.get("configJsonFilePath"))
        config = json.loads(config_file)

        ## SET THE SECRETS TO THE READER CONFIG OPTIONS
        reader_config_options["cloudFiles.subscriptionId"] = config[cloud_file_notifications_config.get("cloudFiles.subscriptionId")]
        reader_config_options["cloudFiles.resourceGroup"] = config[cloud_file_notifications_config.get("cloudFiles.resourceGroup")]

        reader_config_options["cloudFiles.clientId"] = dbutils.secrets.get(
            config[cloud_file_notifications_config.get("kvSecretScope")] 
            ,config[cloud_file_notifications_config.get("cloudFiles.clientId")])

        reader_config_options["cloudFiles.clientSecret"] = dbutils.secrets.get(
            config[cloud_file_notifications_config.get("kvSecretScope")] 
            ,config[cloud_file_notifications_config.get("cloudFiles.clientSecret")])

        reader_config_options["cloudFiles.tenantId"] = dbutils.secrets.get(
            config[cloud_file_notifications_config.get("kvSecretScope")] 
            ,config[cloud_file_notifications_config.get("cloudFiles.tenantId")])
        
        return reader_config_options

    
    def read_dlt_cloud_files(self) -> DataFrame:
        """Read dlt cloud files.

        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        input_df = None
        source_path = self.source_details["path"]
        reader_config_options = self.reader_config_options
        cloud_file_notifications_config = self.cloud_file_notifications_config
        
        if "cloudFiles.useNotifications" in reader_config_options and reader_config_options["cloudFiles.useNotifications"] == "true":
            reader_config_options = PipelineReaders.set_cloud_file_notification_config(self.spark,reader_config_options,cloud_file_notifications_config)

        if self.schema_json and self.source_format.lower() != "delta":
            schema = StructType.fromJson(self.schema_json)
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .schema(schema)
                .load(source_path)
            )
        else:
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .load(source_path)
            )

        #TODO: Add cloudfiles metadata

        # if self.source_details and "source_metadata" in self.source_details.keys():
        #     input_df = PipelineReaders.add_cloudfiles_metadata(self.source_details, input_df)
        return input_df

    # @staticmethod
    # def add_cloudfiles_metadata(sourceDetails, input_df):
    #     source_metadata_json = json.loads(sourceDetails.get("source_metadata"))
    #     keys = source_metadata_json.keys()
    #     autoloader_metadata_column_flag = False
    #     source_metadata_col_name = "_metadata"
    #     input_df = input_df.selectExpr("*", f"{source_metadata_col_name}")
    #     if "select_metadata_cols" in source_metadata_json:
    #         select_metadata_cols = source_metadata_json["select_metadata_cols"]
    #         for select_metadata_col in select_metadata_cols:
    #             input_df = input_df.withColumn(select_metadata_col, col(select_metadata_cols[select_metadata_col]))
    #     if "include_autoloader_metadata_column" in keys:
    #         autoloader_metadata_column = source_metadata_json["include_autoloader_metadata_column"]
    #         autoloader_metadata_column_flag = True if autoloader_metadata_column.lower() == "true" else False
    #         if autoloader_metadata_column_flag and "autoloader_metadata_col_name" in source_metadata_json:
    #             custom_source_metadata_col_name = source_metadata_json["autoloader_metadata_col_name"]
    #             if custom_source_metadata_col_name != source_metadata_col_name:
    #                 input_df = input_df.withColumnRenamed(f"{source_metadata_col_name}",
    #                                                       f"{custom_source_metadata_col_name}")
    #         elif autoloader_metadata_column_flag and "autoloader_metadata_col_name" not in source_metadata_json:
    #             input_df = input_df.withColumnRenamed("_metadata", "source_metadata")
    #     else:
    #         input_df = input_df.drop(f"{source_metadata_col_name}")
    #     return input_df
    # @staticmethod

    def read_dlt_delta(self) -> DataFrame:
        """Read dlt delta.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        reader_config_options = self.reader_config_options

        if reader_config_options and len(reader_config_options) > 0:
            return (
                self.spark.readStream.options(**reader_config_options).table(
                    f"""{self.source_details["source_database"]}
                        .{self.source_details["table"]}"""
                )
            )
        else:
            return (
                self.spark.readStream.table(
                    f"""{self.source_details["source_database"]}
                        .{self.source_details["table"]}"""
                )
            )


    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils # pylint: disable=import-error disable=import-outside-toplevel # type: ignore
        return DBUtils(self.spark)


    def read_kafka(self) -> DataFrame:
        """Read eventhub with dataflowspec and schema.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        if self.source_format == "eventhub":
            kafka_options = self.get_eventhub_kafka_options()
        elif self.source_format == "kafka":
            kafka_options = self.get_kafka_options()
        raw_df = (
            self.spark
            .readStream
            .format("kafka")
            .options(**kafka_options) # pylint: disable=possibly-used-before-assignment
            .load()
            # add date, hour, and minute columns derived from eventhub enqueued timestamp
            .selectExpr("*", "to_date(timestamp) as date", "hour(timestamp) as hour", "minute(timestamp) as minute")
        )
        if self.schema_json:
            schema = StructType.fromJson(self.schema_json)
            return (
                raw_df.withColumn("parsed_records", from_json(col("value").cast("string"), schema))
            )
        else:
            return raw_df


    def get_eventhub_kafka_options(self):
        """Get eventhub options from dataflowspec."""
        dbutils = PipelineReaders.get_db_utils(self.spark)
        eh_namespace = self.source_details.get("eventhub.namespace")
        eh_port = self.source_details.get("eventhub.port")
        eh_name = self.source_details.get("eventhub.name")
        eh_shared_key_name = self.source_details.get("eventhub.accessKeyName")
        secret_name = self.source_details.get("eventhub.accessKeySecretName")
        if not secret_name:
            # set default value if "eventhub.accessKeySecretName" is not specified
            secret_name = eh_shared_key_name
        secret_scope = self.source_details.get("eventhub.secretsScopeName")
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
        kafka_options = {**eh_conn_options, **self.reader_config_options}
        return kafka_options

    def get_kafka_options(self):
        """Get kafka options from dataflowspec."""
        source_details_map = self.source_details
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
                dbutils = PipelineReaders.get_db_utils(self.spark)
                kafka_ssl_conn = {
                    "kafka.ssl.truststore.location": ssl_truststore_location,
                    "kafka.ssl.keystore.location": ssl_keystore_location,
                    "kafka.ssl.keystore.password": dbutils.secrets.get(keystore_scope, keystore_key),
                    "kafka.ssl.truststore.password": dbutils.secrets.get(truststore_scope, truststore_key)
                }
                kafka_options = {**kafka_base_ops, **kafka_ssl_conn, **self.reader_config_options}
            else:
                params = ["kafka.ssl.truststore.secrets.scope",
                          "kafka.ssl.truststore.secrets.key",
                          "kafka.ssl.keystore.secrets.scope",
                          "kafka.ssl.keystore.secrets.key"
                          ]
                raise ValueError(f"Kafka ssl required params are: {params}! provided options are :{source_details_map}")
        else:
            kafka_options = {**kafka_base_ops, **self.reader_config_options}
        return kafka_options