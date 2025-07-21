# import os
import pyspark.sql.functions as f
# from triton.utils.spark.etl_helpers import get_current_user
from datetime import *
import sys
from pyspark import SQLContext, SparkContext, SparkConf, HiveContext
from pyspark.sql import HiveContext,DataFrame as spark_df
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
# from dateutil.relativedelta import relativedelta
# from functools import reduce
# import pandas as pd

# def get_last_partition(hc,table):
#     last_partition = (hc.sql("show partitions "+table)
#                       .orderBy(desc("partition")).select("partition").collect()[0][0])
#     return last_partition.split('=')[1].strip()

# def process_data(spark, env):
#     hc = HiveContext(spark)

conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark = SparkSession.builder \
    .appName("merge_revenue_ifrs_dd_seized") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")


    #define table
table_1 = 'testing.ifrs_bonus_seizure_poc_tokenized'

    #define periode
#event_date = "'2025-04-01'"
#load_date  = "'2025-04-02'"

#print(f"""run for event_date={event_date} and load_date={load_date}""")

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]

# define periode
load_date = f"'{argv1}'"
event_date  = f"'{argv2}'"

print(f"""run for load_date={load_date} and event_date={event_date}""")

QUERY = f"""
    SELECT cast (trx_date as DATE) AS trx_date,
           concat(substr(timestamp_ifrs,1,4), '-', substr(timestamp_ifrs,5,2), '-', substr(timestamp_ifrs,7,2)) AS purchase_date,
           transaction_id,
           subs_id,
           msisdn,
           price_plan_id,
           brand,
           pre_post_flag,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
           lac,
           ci,
           lacci_id_ifrs AS lacci_id,
           node,
           CASE WHEN area_sales IS NULL OR area_sales='' THEN 'UNKNOWN' ELSE area_sales END AS area_sales,
           CASE WHEN region_sales IS NULL OR region_sales='' THEN 'UNKNOWN' ELSE region_sales END AS region_sales,
           CASE WHEN branch IS NULL OR branch='' THEN 'UNKNOWN' ELSE branch END AS branch,
           CASE WHEN sub_branch IS NULL OR sub_branch='' THEN 'UNKNOWN' ELSE sub_branch END AS subbranch,
           CASE WHEN cluster_sales IS NULL OR cluster_sales='' THEN 'UNKNOWN' ELSE cluster_sales END AS cluster_sales,
           CASE WHEN propinsi IS NULL OR propinsi='' THEN 'UNKNOWN' ELSE propinsi END AS provinsi,
           CASE WHEN kabupaten IS NULL OR kabupaten='' THEN 'UNKNOWN' ELSE kabupaten END AS kabupaten,
           CASE WHEN kecamatan IS NULL OR kecamatan='' THEN 'UNKNOWN' ELSE kecamatan END AS kecamatan,
           CASE WHEN kelurahan IS NULL OR kelurahan='' THEN 'UNKNOWN' ELSE kelurahan END AS kelurahan,
           lacci_closing_flag,
           sigma_business_id,
           sigma_rules_id,
           sku,
           l1_payu,
           l2_service_type,
           l3_allowance_type,
           l4_product_category,
           l5_product,
           '' AS l1_ias,
           '' AS l2_ias,
           '' AS l3_ias,
           commercial_name,
           channel,
           validity AS pack_validity,
           cast(sum(0) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(rev_seized) AS decimal (38,15)) AS rev_seized,
           cast(sum(0) AS int) AS dur,
           cast(sum(remaining_quota) AS int) AS trx,
           cast(sum(0) AS bigint) AS vol,
           cust_id,
           '' AS profile_name,
           item_id AS quota_name,
           '' AS service_filter,
           '' AS price_plan_name,
           channel_id,
           '' AS site_id,
           '' AS site_name,
           region_hlr,
           city_hlr,
           {load_date} AS load_date,
           cast (event_Date as DATE) AS event_date,
           'SEIZED' AS source

    FROM {table_1}
    WHERE event_date = {event_date}
      AND rev_seized < 100000000000
      AND pre_post_flag = '1'

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
df.createOrReplaceTempView("temp_merge_revenue_ifrs_dd_seized")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_revenue_ifrs_dd_seized
""")
