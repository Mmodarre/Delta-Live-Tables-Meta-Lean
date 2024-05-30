import datetime
import helpers.populate_md as pm

dataFlowId = '100-Customers'
dataFlowGroup = "B1"
sourceFormat = "delta"
sourceDetails = {"database":"mehdidatalake_catalog.retail_cdc","table":"customers_dlt_meta"}
readerConfigOptions = None
targetFormat = 'delta'
targetDetails = {"database":"mehdidatalake_catalog.retail_cdc","table":"customers_dlt_meta_silver"}
tableProperties = None
selectExp = ["cast(customers_id as String) as customer_id","address","email","firstname as first_name","lastname as last_name","operation","TO_TIMESTAMP(operation_date, 'MM-dd-yyyy HH:mm:ss') as operation_date","file_path","current_timestamp() as processing_time"]
whereClause = None
partitionColumns = None
cdcApplyChanges = '{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}'
dataQualityExpectations = '{"expect_or_drop": {"valid_customer_id": "customers_id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
version = "v1"
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
## Need to fix
createdBy = current_user()
updatedBy = current_user()
silver_MD_TABLE = 'mehdidatalake_catalog.dlt_meta_dataflowspecs_1.s_test'



pm.populate_silver(silver_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns, cdcApplyChanges, dataQualityExpectations, version,createDate, createdBy,updateDate, updatedBy)