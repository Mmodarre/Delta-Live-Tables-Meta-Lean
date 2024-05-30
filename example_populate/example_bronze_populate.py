import datetime
import helpers.populate_md as pm


dataFlowId = '101-Items'
dataFlowGroup = "B1"
sourceFormat = "cloudFiles"

sourceDetails = {"path":"/Volumes/mehdidatalake_catalog/retail_cdc/retail_landing/cdc_raw/items","source_database":"files","source_table":"items"}
schema_hints = "brand.name STRING, gender STRING, hassimilarproducts BOOLEAN, id INTEGER, merchandiselabel STRING, merchantid INTEGER, priceinfo.currencyCode STRING, priceInfo.discountLabel STRING, priceInfo.finalPrice INTEGER, priceInfo.formattedFinalPrice STRING, priceInfo.initialPrice INTEGER, shortDescription STRING, stockTotal INTEGER"
readerConfigOptions = {"cloudFiles.format":"csv","cloudFiles.rescuedDataColumn":"_rescued_data","cloudFiles.inferColumnTypes":"true","header":"true","cloudFiles.schemaEvolutionMode":"addNewColumns"}#,"cloudFiles.schemaHints":schema_hints}
cloudFileNotificationsConfig = None

schema = None #'{"type":"struct","fields": [{"name":"brand.name","type":"string","nullable": true,"metadata":{}}, {"name":"gender","type":"string","nullable": true,"metadata":{}}, {"name":"hassimilarproducts","type":"boolean","nullable": true,"metadata":{}}, {"name":"id","type":"integer","nullable": true,"metadata":{}}, {"name":"merchandiselabel","type":"string","nullable": true,"metadata":{}}, {"name":"merchantid","type":"integer","nullable": true,"metadata":{}}, {"name":"priceinfo.currencyCode","type":"string","nullable": true,"metadata":{}}, {"name":"priceInfo.discountLabel","type":"string","nullable": true,"metadata":{}}, {"name":"priceInfo.finalPrice","type":"integer","nullable": true,"metadata":{}}, {"name":"priceInfo.formattedFinalPrice","type":"string","nullable": true,"metadata":{}}, {"name":"priceInfo.initialPrice","type":"integer","nullable": true,"metadata":{}}, {"name":"shortDescription","type":"string","nullable": true,"metadata":{}}, {"name":"stockTotal","type":"integer","nullable": true,"metadata":{}}, {"name":"_rescued_data","type":"string","nullable": true,"metadata":{}}]}'

targetFormat = 'delta'
targetDetails = {"database":"mehdidatalake_catalog.retail_cdc","table":"items_dlt_meta_meta"}
tableProperties = {"comment":"New items data incrementally ingested from cloud object storage landing zone"}
partitionColumns = None
cdcApplyChanges = None
dataQualityExpectations = '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_product_id": "id IS NOT NULL"}}'
quarantineTargetDetails = None
quarantineTableProperties = None
version = "v1"
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = current_user()
updatedBy = current_user()
BRONZE_MD_TABLE = 'mehdidatalake_catalog.dlt_meta_dataflowspecs_1.b_test'



pm.populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,version,createDate,createdBy,updateDate,updatedBy)