##findspark.add_packages('mysql:mysql-connector-java:8.0.33')
import datetime
import shutil

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, FloatType, DateType

from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))

s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

print(response)

logger.info("List of Buckets: %s", response['Buckets'])

# check if local directory has a file
# If file is there then check if file is present in the staging area
# with status as 'A' Then don't delete and try to rerun
# else give an error and not process next file

csv_files = [file for file in os.listdir(config.local_directory)
             if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    statement = f"""
            select distinct file_name from 
            {config.database_name}.{config.product_staging_table} 
            where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A' 
        """
    #statement = f"select distinct file_name from "\
    #           f"youtube_project.product_staging_table "\
    #            f"where file_name in ({str(total_csv_files)[1:-1]}) and status = 'I' "

    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run failed Please check")
    else:
        logger.info("No record match")

else:
    logger.info("Last run was successful!!!")


try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, bucket_name=config.bucket_name, folder_path=folder_path)
    logger.info("Absolute path in S3 for csv files are %s ", s3_absolute_file_path)

    if not s3_absolute_file_path:
        logger.info(f"No files found in S3 bucket '{config.bucket_name}' in folder '{folder_path}'")
        raise Exception(f"No files found in S3 bucket '{config.bucket_name}' in folder '{folder_path}'")

except Exception as e:
    logger.error(f"An error occurred while creating S3Reader: {e}")
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info("File path available on S3 under %s bucket and folder name is %s", bucket_name, file_paths)

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"An error occurred while downloading files from S3: {e}")
    sys.exit()


# Get a list of files in the local directory

all_files = os.listdir(local_directory)
logger.info("List of downloaded files in the local directory: %s", all_files)

# Get a list of CSV files in the local directory

if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.join(local_directory, file))
        else:
            error_files.append(os.path.join(local_directory, file))

    if not csv_files:
        logger.error("No CSV files found in the local directory")
        raise Exception("No CSV files found in the local directory")

else:
    logger.error("No files found in the local directory")
    raise Exception("No files found in the local directory")

### make csv lines convert into a list of comma separated ###
logger.info("********** Listing csv files ************")
logger.info("CSV files found in the local directory: %s", csv_files)

spark = spark_session()
logger.info("********** Spark session created ************")

# Check the required column in the schema of the csv file
# If the required column, keep it in a list or error_files
# else union all the data into one dataframe

logger.info("********** Checking the required columns in the schema of the csv file ************")

corrected_files = []

for data in csv_files:
    data_schema = spark.read.format("csv")\
     .option("header", "true")\
     .load(data).columns
    logger.info("Schema of the csv file %s is %s", data, data_schema)
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info("Missing columns in the csv file %s", missing_columns)


    if missing_columns:
        logger.error("Missing columns in the csv file %s", missing_columns)
        error_files.append(data)
    else:
        corrected_files.append(data)

logger.info("********** List of Corrected files ************")
logger.info("Corrected files are %s", corrected_files )
logger.info(f"Error files are {error_files}")
logger.info("********** Moving error data to error folder if any ************")

# Move the error files to the error folder
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_path}' from s3 file path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(message)
        else:
            logger.error(f"File '{file_path}' does not exist in the local directory")
else:
    logger.info("********** No error files found in the local directory ************")


# Additional  columns need to be taken care of
# Determine extra columns in the schema of the csv file

#Before running the process
#stage table needs to be updated with status Active(A) or InActive(I)
logger.info("********** Updating the stage table with status Active(A) or InActive(I) ************")

insert_statement = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if corrected_files:
    for file in corrected_files:
        file_name = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                      f"(file_name, file_location, created_date, status)" \
                      f"VALUES ('{file_name}', '{file_name}', '{formatted_date}', 'A')"

        insert_statement.append(statements)
        logger.info(f"Insert statement for {file_name} is {statements}")
        logger.info("********** Connecting to MySql server ************")

        connection = get_mysql_connection()
        cursor = connection.cursor()
        logger.info("********** MySql server connected ************")
        for statement in insert_statement:
            cursor.execute(statement)
            connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"Data inserted successfully for {file_name}")
else:
    logger.info("********** No files to process ************")
    raise Exception("******** No Data available with correct files to process ********")

logger.info("********** Staging table completed successfully ************")
logger.info("********** Fixing extra column coming from source ************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", StringType(), True),
    StructField("additional_column", StringType(), True)
])


final_df_to_process = spark.createDataFrame([], schema=schema)
#Create a new column with concatenated values of extra columns

# connecting with Database Reader
# Run below 3 lines if above commented code is not working
# database_client = DatabaseReader(config.url, config.properties)
# logger.info("********** Creating empty dataframe ************")
# final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_tble")

for data in corrected_files:
    data_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(data)

    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info("Extra columns in the csv file %s are %s", data, extra_columns)
    logger.info("********** Creating a new column with concatenated values of extra columns ************")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(",", *extra_columns))\
        .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                "price", "quantity", "total_cost", "additional_column")
        logger.info(f"processed {data} and added 'additional_column' to the dataframe")
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
        .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                "price", "quantity", "total_cost", "additional_column")


    final_df_to_process = final_df_to_process.union(data_df)
    logger.info("********** Final Dataframe from source will be going to show ************")
    final_df_to_process.show() # Show the final dataframe

# Enrich the data from all dimension table
# also create a datamart for sales_teaand their incentive, address and others
# another datamart for customer who bought  how much product each days of month
# For every month there should be a file and inside that a store_id segregate
# Read the data from the parquet file and generate a csv file in which
# will have sales_person_name, sales_person_store_id,
# sales_person_total_billing_done_each_month, total_incentive

# concatenating with Database Reader
database_client = DatabaseReader(config.url, config.properties)
# Creating df for all tables
# customer_table
logger.info("********** Loading customer table into customer_table_df ************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# product_table
logger.info("********** Loading product table into product_table_df ************")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# product_staging_table
logger.info("********** Loading product_staging_table into product_staging_table_df ************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales_team_table
logger.info("********** Loading sales_team table into sales_team_table_df ************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store_table
logger.info("********** Loading store table into store_table_df ************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(
                                                       final_df_to_process,
                                                       customer_table_df,
                                                       store_table_df,
                                                       sales_team_table_df)

# Final enriched data
logger.info("********** Final enriched data ************")
s3_customer_store_sales_df_join.show()

# write the customer data into customer_data_mart in parquet format
# File will be written in local first
# Move the file from local to s3 bucket
# Write reporting data into Mysql table also

logger.info("********** Writing customer data into customer_data_mart in parquet format ************")
final_customer_data_mart = s3_customer_store_sales_df_join.select(
    "ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
    "ct.pincode", "phone_number", "sales_date","total_cost")

logger.info("********** Final customer data to be written into customer_data_mart ************")
final_customer_data_mart.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart, config.customer_data_mart_local_file)

logger.info(f"********** Customer data written to local disk at {config.customer_data_mart_local_file} ************")

# Move the file from local to s3 bucket
logger.info("********** Moving the file from local to s3 bucket ************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

# Sales team data mart
logger.info("********** Writing sales team data into sales_team_data_mart in parquet format ************")
final_sales_team_data_mart = s3_customer_store_sales_df_join.select(
    "store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
    "sales_person_address", "sales_person_pincode",
    "sales_date", "total_cost", expr("SUBSTRING(sales_date, 1, 7) as sales_month"))

logger.info("********** Final sales team data to be written into sales_team_data_mart ************")
final_sales_team_data_mart.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart, config.sales_team_data_mart_local_file)
logger.info(f"********** Sales team data written to local disk at {config.sales_team_data_mart_local_file} ************")

# Move data on S3 bucket from sales_data_mart
logger.info("********** Moving the file from local to s3 bucket ************")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Also writing data into partitions
logger.info("********** Writing data into partitions ************")
final_sales_team_data_mart.write.format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .partitionBy("sales_month", "store_id")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()

#Move data on S3 bucket for partitioner folder
s3_prefix = "sales_partition_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# Calculation for customer_mart
# Find out customer total purchase every_month
#write thed data into mysql table

logger.info("********** Calculating customer total purchase every month ************")
customer_mart_calculation_table_write(final_customer_data_mart_df=final_customer_data_mart)
logger.info("********** Customer total purchase every month calculated and written into table successfully ************")

# Calculating sales team mart
# Find out total_sales done by each sales person every month
# Give the top performer 1% incentive of total sales of that month
# No incentive to the remaining sales person
# Write the data into mysql table

logger.info("********** Calculating sales team mart ************")
sales_mart_calculation_table_write(final_sales_team_data_mart_df=final_sales_team_data_mart)
logger.info("********** Sales team mart calculated and written into table successfully ************")

#############  Last step  ################
logger.info("********** Process completed successfully ************")

# Move the file on s3 into processed folder and delete the file from local

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_paths)
logger.info(f"{message}")

logger.info("********** Deleting sales data file from local ************")
delete_local_file(config.local_directory)
logger.info("********** Sales data file deleted from local successfully ************")

logger.info("********** Deleting customer data from local ************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("********** Customer data deleted from local successfully ************")

logger.info("********** Deleting sales team data file from local ************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("********** Sales team data file deleted from local successfully ************")

logger.info("********** Deleting sales team partitioned data file from local ************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********** Sales team partitioned data file deleted from local successfully ************")


# Delete the file from local
for file in all_files:
    os.remove(os.path.join(local_directory, file))
    logger.info(f"Deleted file '{file}' from local directory")

# Update status in stage table
update_statements = []
if corrected_files:
    for file in corrected_files:
        file_name = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                      f" SET status = 'I', updated_date='{formatted_date}'" \
                      f"WHERE file_name = '{file_name}'"

        update_statements.append(statements)

    logger.info(f"Update statement created for staging table -- {update_statements}")
    logger.info("********** Connecting to MySql server ************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("********** MySql server connected successfully ************")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("********** There is some error in process in between ************")
    sys.exit()

input("Press Enter to exit...")





