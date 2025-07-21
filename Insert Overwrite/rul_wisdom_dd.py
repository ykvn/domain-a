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
    .appName("rul_wisdom_dd") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

    #define table
table_1 = 'testing.wisdom_poc_tokenized'
table_2 = 'testing.cb_profile_tnt_poc_tokenized'
table_3 = 'testing.lacci_dom_ifrs_chg_mtd_poc_tokenized'
table_4 = 'testing.lacci_dom_ifrs_chg_mm_poc_tokenized'

    #define periode
#event_date = "'2025-04-01'"
#month_date = "'2025-03-01'"

#print(f"""run for event_date={event_date}""")

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]

# define periode
event_date = f"'{argv1}'"
month_date  = f"'{argv2}'"

print(f"""run for event_date={event_date} and month_date={month_date}""")

QUERY = f"""
    SELECT cast (a.trx_date as DATE) AS trx_date,
           a.transaction_id,
           a.sku,
           a.business_id,
           a.rules_id,
           a.msisdn,
           h.brand,
           h.region_hlr,
           a.non_usage_flag,
           a.allowance_sub_type AS allowance_subtype,
           a.allowances_descriptions,
           a.quota_name,
           a.bonus_apps_id,
           a.profile_name,
           h.cust_type_desc,
           h.cust_subtype_desc,
           h.customer_sub_segment,
           coalesce(b.lacci_id, c.lacci_id) AS lacci_id,
           coalesce(b.area_sales, c.area_sales) AS area_sales,
           coalesce(b.region_sales, c.region_sales) AS region_sales,
           coalesce(b.branch, c.branch) AS branch,
           coalesce(b.subbranch, c.subbranch) AS subbranch,
           coalesce(b.cluster_sales, c.cluster_sales) AS cluster_sales,
           coalesce(b.provinsi, c.provinsi) AS provinsi,
           coalesce(b.kabupaten, c.kabupaten) AS kabupaten,
           coalesce(b.kecamatan, c.kecamatan) AS kecamatan,
           coalesce(b.kelurahan, c.kelurahan) AS kelurahan,
           coalesce(b.node_type, c.node_type) AS node_type,
           a.charge,
           sum(alloc_tp) AS rev,
           cast (a.event_date as DATE) AS event_date

    FROM (
    SELECT * FROM {table_1}
    WHERE event_date = {event_date}
      AND non_usage_flag = '1'
      AND status = 'OK00'
    ) a

    LEFT JOIN (
    SELECT * FROM {table_2}
    WHERE event_date = {event_date}
    ) h
    ON a.event_date = h.event_date
      AND a.msisdn = h.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_date,
           msisdn,
           lacci_id,
           area_sales,
           region_sales,
           branch,
           subbranch,
           cluster_sales,
           provinsi,
           kabupaten,
           kecamatan,
           kelurahan,
           node_type,
           rank() over (partition by event_date, msisdn order by lacci_id desc) AS rnk
    FROM {table_3}
    WHERE event_date = {event_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) b
    ON a.event_date = b.event_date
      AND a.msisdn = b.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_date,
           msisdn,
           lacci_id,
           area_sales,
           region_sales,
           branch,
           subbranch,
           cluster_sales,
           provinsi,
           kabupaten,
           kecamatan,
           kelurahan,
           node_type,
           rank() over (partition by event_date, msisdn order by lacci_id desc) AS rnk
    FROM {table_4}
    WHERE event_date = {month_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) c
    ON a.msisdn = c.msisdn

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31
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
df.createOrReplaceTempView("temp_rul_wisdom_dd")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.rul_wisdom_dd_poc_tokenized
PARTITION (event_date)
SELECT *
FROM temp_rul_wisdom_dd
""")
