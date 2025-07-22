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
    .appName("merge_revenue_ifrs_dd_wisdom") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")


# def process_data(spark, env):
#     hc = HiveContext(spark)
 
#     #define table
#     table_1 = f'{env["table_1"]["database"]}.{env["table_1"]["table"]}'
#     table_2 = f'{env["table_2"]["database"]}.{env["table_2"]["table"]}'
#     table_3 = f'{env["table_3"]["database"]}.{env["table_3"]["table"]}'
#     table_4 = f'{env["table_4"]["database"]}.{env["table_4"]["table"]}'
#     table_5 = f'{env["table_5"]["database"]}.{env["table_5"]["table"]}'
#     table_6 = f'{env["table_6"]["database"]}.{env["table_6"]["table"]}'
#     table_7 = f'{env["table_7"]["database"]}.{env["table_7"]["table"]}'
#     table_8 = f'{env["table_8"]["database"]}.{env["table_8"]["table"]}'

table_1 = 'testing.rul_wisdom_dd_poc_tokenized'
table_2 = 'testing.subs_ocs_spp_poc_tokenized'
table_3 = 'testing.usage_ocs_chg_poc_tokenized'
table_4 = 'testing.ocs_chg_reject_poc_tokenized'
table_5 = 'testing.evoucher_redeem_poc_tokenized'
table_6 = 'testing.evoucher_redeem_reject_poc_tokenized'
table_7 = 'testing.mkios_evcrec_evcvas_ngrs_poc_tokenized'
table_8 = 'testing.mkios_evcrec_evcvas_ngrs_reject_poc_tokenized'

    #define periode
#event_date  = "'2025-04-01'"
#load_date   = "'2025-04-03'"
#first_date  = "'2025-04-01'"
#last_date   = "'2025-04-01'"

#print(f"""run for event_date={event_date} and load_date={load_date}""")

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]
argv3 = sys.argv[3]
argv4 = sys.argv[4]

# define periode
load_date = f"'{argv1}'"
event_date  = f"'{argv2}'"
first_date  = f"'{argv3}'"
last_date  = f"'{argv4}'"

print(f"""run for load_date={load_date} and event_date={event_date} and first_date={first_date} and last_date={last_date}""")

QUERY = f"""
    SELECT CAST(trx_date AS DATE) AS trx_date,
           trx_date AS purchase_date,
           transaction_id,
           '' AS subs_id,
           a.msisdn,
           int(coalesce(c1.offer_id, d1.offer_id, null)) AS price_plan_id,
           brand,
           1 AS pre_post_flag,
           cust_type_desc,
           cust_subtype_desc,
           customer_sub_segment,
           '' AS lac,
           '' AS ci,
           lacci_id,
           '' AS node,
           CASE WHEN area_sales IS NULL OR area_sales='' THEN 'UNKNOWN' ELSE area_sales END AS area_sales,
           CASE WHEN region_sales IS NULL OR region_sales='' THEN 'UNKNOWN' ELSE region_sales END AS region_sales,
           CASE WHEN branch IS NULL OR branch='' THEN 'UNKNOWN' ELSE branch END AS branch,
           CASE WHEN subbranch IS NULL OR subbranch='' THEN 'UNKNOWN' ELSE subbranch END AS subbranch,
           CASE WHEN cluster_sales IS NULL OR cluster_sales='' THEN 'UNKNOWN' ELSE cluster_sales END AS cluster_sales,
           CASE WHEN provinsi IS NULL OR provinsi='' THEN 'UNKNOWN' ELSE provinsi END AS provinsi,
           CASE WHEN kabupaten IS NULL OR kabupaten='' THEN 'UNKNOWN' ELSE kabupaten END AS kabupaten,
           CASE WHEN kecamatan IS NULL OR kecamatan='' THEN 'UNKNOWN' ELSE kecamatan END AS kecamatan,
           CASE WHEN kelurahan IS NULL OR kelurahan='' THEN 'UNKNOWN' ELSE kelurahan END AS kelurahan,
           CAST(null AS INT) AS lacci_closing_flag,
           business_id AS sigma_business_id,
           rules_id AS sigma_rules_id,
           sku,
           '' AS l1_payu,
           '' AS l2_service_type,
           allowance_subtype AS l3_allowance_type,
           '' AS l4_product_category,
           '' AS l5_product,
           '' AS l1_ias,
           '' AS l2_ias,
           '' AS l3_ias,
           '' AS commercial_name,
           '' AS channel,
           '' AS pack_validity,
           cast(sum(rev) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(0) AS decimal (38,15)) AS rev_seized,
           cast(sum(0) AS int) AS dur,
           cast(count(distinct transaction_id) AS int) AS trx,
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
           cast(a.event_Date AS DATE) AS event_date,
           'WISDOM' AS source

    FROM (
    SELECT * FROM {table_1}
    WHERE event_date = {event_date}
      AND coalesce(lower(brand), 'prepaid') <> 'kartuhalo'
    ) a

    LEFT JOIN (
    SELECT event_Date,
           msisdn,
           provider_id AS offer_id
    FROM {table_2}
    WHERE event_Date = {event_date}
    GROUP BY 1,2,3
    ) c1 
    ON a.event_Date = c1.event_Date
      AND a.msisdn = c1.msisdn

    LEFT JOIN (
    SELECT charging_id,
           'chg' AS SOURCE,
           provider_id AS offer_id
    FROM {table_3}
    WHERE event_date BETWEEN {first_date} AND {last_date}
      AND pre_post_flag = '1'
    GROUP BY 1,2,3

    UNION ALL

    SELECT charging_id,
           'chg' AS SOURCE,
           provider_id AS offer_id
    FROM {table_4}
    WHERE event_date = current_date
      AND trx_date BETWEEN {first_date} AND {last_date}
      AND pre_post_flag = '1'
    GROUP BY 1,2,3
           
    UNION ALL

    SELECT dc_order_id AS charging_id,
           'evoucher' AS SOURCE,
           offer_id
    FROM {table_5}
    WHERE event_date BETWEEN {first_date} AND {last_date}
      AND status = '3'
      AND redeem_state = '3'
      AND bid_flag IS NULL
    GROUP BY 1,2,3

    UNION ALL

    SELECT dc_order_id AS charging_id,
           'evoucher' AS SOURCE,
           offer_id
    FROM {table_6}
    WHERE event_date = current_date
      AND trx_date BETWEEN {first_date} AND {last_date}
      AND status = '3'
      AND redeem_state = '3'
      AND bid_flag IS NULL
    GROUP BY 1,2,3

    UNION ALL

    SELECT dc_orderid AS charging_id,
           'ngrs' AS SOURCE,
           offer_id
    FROM {table_7}
    WHERE event_date BETWEEN {first_date} AND {last_date}
      AND upper(processing_code) in ('RESERVESTOCKPACKAGEWITHEMONEYTP',
                                     'RECHARGEREVERSAL',
                                     'RESERVESTOCKPACKAGEWITHEMONEY',
                                     'MODERNSTOCKRECHARGEPACKAGE',
                                     'STOCKRECHARGEPACKAGEFIXED',
                                     'STOCKRECHARGEPACKAGE')
    GROUP BY 1,2,3

    UNION ALL

    SELECT dc_orderid AS charging_id,
           'ngrs' AS SOURCE,
           offer_id
    FROM {table_8}
    WHERE event_date = current_date
      AND trx_date BETWEEN {first_date} AND {last_date}
      AND upper(processing_code) in ('RESERVESTOCKPACKAGEWITHEMONEYTP',
                                     'RECHARGEREVERSAL',
                                     'RESERVESTOCKPACKAGEWITHEMONEY',
                                     'MODERNSTOCKRECHARGEPACKAGE',
                                     'STOCKRECHARGEPACKAGEFIXED',
                                     'STOCKRECHARGEPACKAGE')
    GROUP BY 1,2,3
    ) d1 
    ON substr(a.transaction_id,1,32) = substr(d1.charging_id, 1, 32)

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
df.createOrReplaceTempView("temp_merge_wisdom")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_wisdom
""")
