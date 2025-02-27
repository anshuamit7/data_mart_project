from pyspark.sql import *
from pyspark.sql.connect.functions import substring
from pyspark.sql.functions import window, concat, col, lit, rank, when
from pyspark.sql.window import *
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


# calculation for customer datamart
# find out the customer total purchase every month
# write the data to mysql database

def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
    final_sales_team_data_mart = final_sales_team_data_mart_df \
        .withColumn("sales_month", substring(col("sales_date"),1,7)) \
        .withColumn("total_sales_every_month", sum(col("total_cost")).over(window)) \
        .select("store_id", "sales_person_id",
                concat(col("sales_person_first_name"), lit(" "),
                       col("sales_person_last_name")).alias("full_name"),
                "sales_month", "total_sales_every_month") \
        .distinct()

    rank_window = (Window.partitionBy("store_id", "sales_month")
                   .orderBy(col("total_sales_every_month").desc()))
    final_sales_team_data_mart_table = final_sales_team_data_mart \
        .withColumn("rank", rank().over(rank_window)) \
    .withColumn("incentive", when(col("rank") == 1,
                    col("total_sales_every_month") * 0.01).otherwise(lit(0))) \
        .withColumn("incentive", round(col("incentive"), 2)) \
        .withColumn("total_sales", col("total_sales_every_month")) \
        .select("store_id", "sales_person_id", "full_name",
                "sales_month", "total_sales", "incentive")

    # write the data to mysql customer_data_mart table
    print("Writing data into sales_team  data mart")
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table, config.sales_team_data_mart_table)