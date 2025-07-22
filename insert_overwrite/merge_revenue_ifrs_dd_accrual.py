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

# spark = SparkSession.builder.appName('merge_revenue_ifrs_dd_accrual').getOrCreate()
# def get_last_partition(hc,table):
#     last_partition = (hc.sql("show partitions "+table)
#                       .orderBy(desc("partition")).select("partition").collect()[0][0])
#     return last_partition.split('=')[1].strip()

conf = SparkConf()
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark = SparkSession.builder \
    .appName("merge_revenue_ifrs_dd_accrual") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

table_1 = "testing.ifrs_accrual_product_daily_poc_tokenized"

#define periode
#load_date  = "'2025-04-03'"
#event_date = "'2025-04-01'" --loaddate -1

# get arguments
argv1 = sys.argv[1]
argv2 = sys.argv[2]

# define periode
load_date = f"'{argv1}'"
event_date  = f"'{argv2}'"

print(f"""run for load_date={load_date} and event_date={event_date}""")



QUERY = f"""
    SELECT  CAST(date_sub(trx_date,1) AS DATE) AS trx_date,
           CASE WHEN substr(transaction_id, 3, 3) = '000' 
                THEN substr(from_utc_timestamp(cast(conv(substr(transaction_id,3,11), 16, 10)*1000 AS timestamp), 'Asia/Jakarta'),1,10) 
                ELSE substr(from_utc_timestamp(cast(conv(substr(transaction_id,3,11), 16, 10) AS timestamp), 'Asia/Jakarta'),1,10)
           END AS purchase_date,
           transaction_id,
           subscriber_id AS subs_id,
           msisdn,
           int(price_plan_id) price_plan_id,
           brand,
           2 AS pre_post_flag,
           cust_type_desc,
           cust_subtype_desc,
           customer_group AS customer_sub_segment,
           lac,
           ci,
           lacci_id,
           node,
           CASE WHEN area_sales IS NULL OR area_sales='' THEN 'UNKNOWN' ELSE area_sales END AS area_sales,
           CASE WHEN region_sales IS NULL OR region_sales='' THEN 'UNKNOWN' ELSE region_sales END AS region_sales,
           CASE WHEN branch IS NULL OR branch='' THEN 'UNKNOWN' ELSE branch END AS branch,
           CASE WHEN subbranch IS NULL OR subbranch='' THEN 'UNKNOWN' ELSE subbranch END AS subbranch,
           CASE WHEN cluster_sales IS NULL OR cluster_sales='' THEN 'UNKNOWN' ELSE cluster_sales END AS cluster_sales,
           CASE WHEN provinsi IS NULL OR provinsi='' THEN 'UNKNOWN' ELSE provinsi END AS provinsi,
           CASE WHEN kabupaten IS NULL OR kabupaten='' THEN 'UNKNOWN' ELSE kabupaten END AS kabupaten,
           CASE WHEN kecamatan IS NULL OR kecamatan='' THEN 'UNKNOWN' ELSE kecamatan END AS kecamatan,
           CASE WHEN kelurahan IS NULL OR kelurahan='' THEN 'UNKNOWN' ELSE kelurahan END AS kelurahan,
           int(lacci_closing_flag) lacci_closing_flag,
           sigma_business_id,
           sigma_rules_id,
           substr(transaction_id,19,13) AS sku,
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
           cast(sum(rev) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(0) AS decimal (38,15)) AS rev_seized,
           cast(sum(0) AS int) AS dur,
           cast(count(distinct transaction_id) AS int) AS trx,
           cast(sum(0) AS bigint) AS vol,
           int(customer_id) AS cust_id,
           charge_code AS profile_name,
           amdd_charge_code AS quota_name,
           proration_ind AS service_filter,
           offer_name AS price_plan_name,
           channel_id,
           '' AS site_id,
           '' AS site_name,
           region_hlr,
           city_hlr,
           {load_date} AS load_date,
           cast (date_sub(event_Date,1) as DATE) AS event_date,
           'ACCRUAL' AS source

    FROM {table_1}
    WHERE event_date = date_sub({load_date},1)
      AND amdd_charge_code = 'SFEE'

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,45,46,47,48,49,50,51,52,53,54,55,56,57
    """


df = spark.sql(QUERY)



# Save to Hive
# df.repartition(10) \
#   .write \
#   .mode("overwrite") \
#   .insertInto("testing.merge_revenue_ifrs_dd_poc_tokenized")

# df.write.mode("overwrite").partitionBy("load_date", "event_date", "source").format("parquet").saveAsTable("testing.merge_revenue_ifrs_dd_poc_tokenized")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("temp_merge_accrual")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_accrual
""")
