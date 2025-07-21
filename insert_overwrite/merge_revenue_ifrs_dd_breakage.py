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


conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark = SparkSession.builder \
    .appName("merge_revenue_ifrs_dd_breakage") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

# def process_data(spark, env):
#     hc = HiveContext(spark)

    #define table
table_1 = "testing.stg_breakage_poc_tokenized"
table_2 = "testing.merge_revenue_ifrs_dd_poc_tokenized"
table_3 = "testing.merge_revenue_dd_poc_tokenized"
table_4 = "testing.breakage_reference_poc_tokenized"

    #define periode
# event_date = f'{env["table_1"]["filter_d2"]}'
# load_date  = f'{env["table_1"]["filter_d0"]}'
# month_date = f'{env["table_2"]["filter_month"]}'

#event_date = "'2025-04-01'"
#load_date = "'2025-04-01'"
#month_date = "'2025-03-01'"

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]
argv3 = sys.argv[3]

# define periode
load_date = f"'{argv1}'"
event_date  = f"'{argv2}'"
month_date  = f"'{argv3}'"

print(f"""run for load_date={load_date} and event_date={event_date} and month_date={month_date}""")

#print(f"""run for event_date={event_date} and load_date={load_date}""")


QUERY = f"""
    SELECT CAST (trx_date as DATE) AS trx_date,
           trx_date AS purchase_date,
           transaction_id,
           '' AS subs_id,
           a.msisdn,
           int(c1.price_plan_id) price_plan_id,
           brand,
           1 AS pre_post_flag,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
           '' AS lac,
           '' AS ci,
           lacci_id,
           '' AS node,
           area_sales,
           a.region_sales,
           branch,
           subbranch,
           cluster_sales,
           provinsi,
           kabupaten,
           kecamatan,
           kelurahan,
           CAST(null AS INT) AS lacci_closing_flag,
           business_id AS sigma_business_id,
           rules_id AS sigma_rules_id,
           sku,
           '' AS l1_payu,
           '' AS l2_service_type,
           a.allowance_sub_type AS l3_allowance_type,
           '' AS l4_product_category,
           '' AS l5_product,
           '' AS l1_ias,
           '' AS l2_ias,
           '' AS l3_ias,
           '' AS commercial_name,
           '' AS channel,
           '' AS pack_validity,
           cast(sum(breakage*allocation_rate) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(0) AS decimal (38,15)) AS rev_seized,
           cast(sum(0) AS int) AS dur,
           cast(sum(0) AS int) AS trx,
           cast(sum(0) AS bigint) AS vol,
           CAST(null AS INT) AS cust_id,
           profile_name,
           quota_name,
           '' AS service_filter,
           '' AS price_plan_name,
           '' AS channel_id,
           '' AS site_id,
           '' AS site_name,
           region_hlr,
           '' AS city_hlr,
           {load_date} AS load_date,
           CAST (a.event_Date as DATE) AS event_date,
           source

    FROM (
    SELECT trx_date,
           transaction_id,
           msisdn,
           brand,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
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
           business_id,
           rules_id,
           sku,
           allowance_sub_type,
           allowance,
           profile_name,
           quota_name,
           region_hlr,
           event_Date,
           sum(breakage) AS breakage,
           sum(rev_seized) AS rev_seized,
           SOURCE

    FROM (
    SELECT trx_date,
           transaction_id,
           msisdn,
           brand,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
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
           business_id,
           rules_id,
           sku,
           allowance_sub_type,
           allowance,
           profile_name,
           quota_name,
           region_hlr,
           event_Date,
           sum(breakage) AS breakage,
           '0' AS rev_seized,
           'BREAKAGE' AS SOURCE
    FROM {table_1} a
    WHERE event_Date = {event_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28

    UNION ALL

    SELECT trx_date,
           transaction_id,
           msisdn,
           brand,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
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
           sigma_business_id AS business_id,
           sigma_rules_id AS rules_id,
           sku,
           l3_allowance_type AS allowance_sub_type,
           CASE WHEN upper(l3_allowance_type) like '%DATA%'
                  OR upper(l3_allowance_type) like '%UPCC%' THEN 'DATA'
                WHEN upper(l3_allowance_type) like '%SMS%' THEN 'SMS'
                WHEN upper(l3_allowance_type) like '%VOICE%' THEN 'VOICE'
           ELSE 'NONUSAGE' END AS allowance,
           profile_name,
           quota_name,
           region_hlr,
           event_date,
           sum(rev_seized*-1) AS breakage,
           '0' AS rev_seized,
           'BREAKAGE-SEIZED' AS SOURCE
    FROM {table_2}
    WHERE event_Date = {event_date}
      AND substr(purchase_date, 1, 7) = {month_date}
      AND SOURCE in ('SEIZED','UPCC-SEIZED')
      AND pre_post_flag = '1'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28
    ) a
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,29
    ) a

    LEFT JOIN (
    SELECT event_date,
           msisdn,
           offer_id AS price_plan_id
    FROM {table_3}
    WHERE event_date = {event_date}
      AND pre_post_flag = '1'
    GROUP BY 1,2,3
    ) c1
    ON a.event_date = c1.event_date
      AND a.msisdn = c1.msisdn

    INNER JOIN (
    SELECT * FROM {table_4}
    WHERE event_date IN (SELECT MAX(event_date) FROM {table_4})
    ) b1
    ON a.allowance = b1.allowance_type
      AND a.region_sales = b1.region

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,45,46,47,48,49,50,51,52,53,54,55,56,57
    """

# Run query
df = spark.sql(QUERY)


# Save to Hive
# df.repartition(10) \
#   .write \
#   .mode("overwrite") \
#   .insertInto("testing.merge_revenue_ifrs_dd_poc_tokenized")

# df.write.mode("overwrite").partitionBy("load_date", "event_date", "source").format("parquet").saveAsTable("testing.merge_revenue_ifrs_dd_poc_tokenized")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("temp_merge_revenue_ifrs_dd_breakage")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_revenue_ifrs_dd_breakage
""")
