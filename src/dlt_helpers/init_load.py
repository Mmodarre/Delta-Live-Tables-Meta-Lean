from pyspark.sql.types import *
from pyspark.sql.functions import lit

##  Function to perform initial load
##  The function takes a list of tables to perform initial load
## @param initalLoadTableList: List of tables to perform initial load
## @type initalLoadTableList: List
## @return None
def perform_initial_load(initalLoadTableList=[]):
  ## Data Format Variable
  data_format = 'parquet'
  ## Loop through the list of tables to perform initial load
  for table in initalLoadTableList:
    ## Read the seed table
    df_seed = spark.read.table(table["seed_table"])

    ## Read the DLT landing folder
    df_dlt = spark.read.format(data_format).load(table["dlt_landing_folder"])
    ## variable to hold columns to exclude from seed table
    exclude_colunms = []
    
    
    ## Loop through the schema of the seed table and check if the column is not in the DLT table
    ## all to lower case to avoid case sensitivity
    for i in df_seed.schema.fields:
      if i.name.lower() not in [x.lower() for x in df_dlt.schema.fieldNames()]:
        exclude_colunms.append(i.name)

    ## If it is not, add it to the list of columns to exclude
    ## This is to handle the case where the column is in the seed table but not in the DLT table
    if exclude_colunms and len(exclude_colunms) > 0 :
      print(f"Removing {exclude_colunms} from {table['seed_table']}")
      df_seed = df_seed.drop(*exclude_colunms)

    ## Loop through the schema of the dlt table and check if the column is not in the seed table
    ## If it is not, add it to the list of columns to exclude
    ## This is to handle the case where the column is in the DLT table but not in the seed table
    for i in df_dlt.schema.fields:
      if i.name.lower() not in [x.lower() for x in df_seed.schema.fieldNames()]:
        print(f"Adding {i.name} from {table['dlt_landing_folder']} to {table['seed_table']}")
        df_seed = df_seed.withColumn(i.name, lit(None).cast(i.dataType))
    
    ## Loop through the schema of the DLT table
    ## This is to handle the case where the column is of different data type in the seed table and DLT table
    for i in df_dlt.schema.fields:
    ## cast all integer types to boolean
    ## This is to handle the case where the column is of type IntegerType in the seed table and BooleanType in the DLT table
      if isinstance(i.dataType, BooleanType):
        df_seed = df_seed.withColumn(i.name,df_seed[i.name].cast("boolean"))
        print(f"Casting {i.name} from IntegerType() to BooleanType in {table['seed_table']}")
    ## cast all decimal types to decimal(38,18)
    ## This is to handle the case where the column is of type DecimalType(any,any) in the seed table,
    ## cast to Decimal(38,18) in the DLT table
      if isinstance(i.dataType, DecimalType):
        df_seed = df_seed.withColumn(i.name,df_seed[i.name].cast("decimal(38,18)"))
        print(f"Casting {i.name} from DecimalType() to Decimal(38,18) in {table['seed_table']}")
      

    ## Reorder df_seed columns to match df_dlt columns
    df_seed = df_seed.select(*df_dlt.columns)
    ## Drop duplicates from the seed table
    df_seed = df_seed.dropDuplicates()
    
    ## Get the primary key column
    pk_col = table["pk_col"]
    
    ## Using Left Anti Join to get the data that is only in the seed table
    data_only_in_seed_table_df = df_seed.join(df_dlt, on=[df_seed[pk_col] == df_dlt[pk_col]], how='leftanti')

    ## Select only columns in df_seed
    data_only_in_seed_table_df = data_only_in_seed_table_df.select(*df_dlt.columns)

    if data_only_in_seed_table_df.count() > 0:
      print(f"Writing {data_only_in_seed_table_df.count()} records to {table['dlt_landing_folder']}")
    ## Write the data that is only in the seed table to the DLT landing folder
      data_only_in_seed_table_df.write.format(data_format).mode("append").save(table["dlt_landing_folder"])
    else:
      print(f"No records to write to {table['dlt_landing_folder']}")


''' 
##EXAMPLE USAGE:

tables_to_initial_load = [
  {"seed_table":"<CATALOG>.<SCHEMA>.<TABLE NAME>",
   "dlt_landing_folder":"/Volumes/<CATALOG>/<SCHEMA></<VOLUME>/<FOLDER>..../<TABLE NAME>/","pk_col":"<PRIMARY KEY COLUMN>"}]

perform_initial_load(tables_to_initial_load)
'''