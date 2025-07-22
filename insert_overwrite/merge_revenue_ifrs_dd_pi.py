#import os
import pyspark.sql.functions as f
#from triton.utils.spark.etl_helpers import get_current_user
from datetime import *
import sys
from pyspark import SQLContext, SparkContext, SparkConf, HiveContext
from pyspark.sql import HiveContext,DataFrame as spark_df
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
#rom dateutil.relativedelta import relativedelta
#rom functools import reduce
# import pandas as pd

#def get_last_partition(hc,table):
  #  last_partition = (hc.sql("show partitions "+table)
    #                  .orderBy(desc("partition")).select("partition").collect()[0][0])
   # return last_partition.split('=')[1].strip()

# def process_data(spark, env):
#     hc = HiveContext(spark)

conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark = SparkSession.builder \
    .appName("merge_revenue_ifrs_dd_pi") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")



    #define table
table_1 = 'testing.revenue_pi_dd_poc_tokenized'

    #define periode
#event_date = "'2025-04-01'"
#load_date  = "'2025-04-03'"

#print(f"""run for event_date={event_date} and load_date={load_date}""")

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]

# define periode
load_date = f"'{argv1}'"
event_date  = f"'{argv2}'"

print(f"""run for load_date={load_date} and event_date={event_date}""")

QUERY = f"""
    SELECT cast (expiry_date as DATE) AS trx_date,
           purchase_date_2 AS purchase_date,
           transaction_id,
           '' AS subs_id,
           msisdn,
           int(reserve1) AS price_plan_id,
           brand,
           1 AS pre_post_flag,
           cust_type AS cust_type_desc,
           cust_subtype AS cust_subtype_desc,
           '' AS customer_sub_segment,
           '' AS lac,
           '' AS ci,
           '' AS lacci_id,
           node_type AS node,
           'UNKNOWN' AS area_sales,
           CASE WHEN region IS NULL OR region='' THEN 'UNKNOWN' ELSE region END AS region_sales,
           CASE WHEN branch IS NULL OR branch='' THEN 'UNKNOWN' ELSE branch END AS branch,
           'UNKNOWN' AS subbranch,
           CASE WHEN cluster IS NULL OR cluster='' THEN 'UNKNOWN' ELSE cluster END AS cluster_sales,
           'UNKNOWN' AS provinsi,
           'UNKNOWN' AS kabupaten,
           'UNKNOWN' AS kecamatan,
           'UNKNOWN' AS kelurahan,
           null AS lacci_closing_flag,
           bid AS sigma_business_id,
           '' AS sigma_rules_id,
           '' AS sku,
           '' AS l1_payu,
           '' AS l2_service_type,
           '' AS l3_allowance_type,
           '' AS l4_product_category,
           '' AS l5_product,
           l1_ias,
           l2_ias,
           l3_ias,
           '' AS commercial_name,
           '' AS channel,
           '' AS pack_validity,
           cast(sum(pi_value_final) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(0) AS decimal (38,15)) AS rev_seized,
           cast(sum(0) AS int) AS dur,
           cast(sum(0) AS int) AS trx,
           cast(sum(0) AS bigint) AS vol,
           null AS cust_id,
           '' AS profile_name,
           '' AS quota_name,
           '' AS service_filter,
           '' AS price_plan_name,
           substr(transaction_id,1,2) AS channel_id,
           '' AS site_id,
           '' AS site_name,
           '' AS region_hlr,
           '' AS city_hlr,
           {load_date} AS load_date,
           cast (expiry_date AS DATE) AS event_date,
           'PI' AS source

    FROM {table_1}
    WHERE expiry_date = {event_date}

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,45,46,47,48,49,50,51,52,53,54,55,56,57
    """

# Run query
df = spark.sql(QUERY)

# Save to Hive
# df.repartition(20) \
#   .write \
#   .mode("overwrite") \
#   .insertInto("testing.merge_revenue_ifrs_dd_poc_tokenized")

# df.write.mode("overwrite").partitionBy("load_date", "event_date", "source").format("parquet").saveAsTable("testing.merge_revenue_ifrs_dd_poc_tokenized")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("temp_merge_revenue_ifrs_dd_pi")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_revenue_ifrs_dd_pi
""")
