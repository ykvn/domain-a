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
    .appName("merge_revenue_ifrs_dd_nbp") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")



    #define table
table_1 = 'testing.merge_revenue_dd_poc_tokenized'
table_2 = 'testing.product_catalog_ifrs_c2c_dd_poc_tokenized'
table_3 = 'testing.merge_revenue_ifrs_dd_poc_tokenized'

    #define periode
#event_date =  "'2025-04-01'"
#load_date  =  "'2025-04-02'"

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
           trx_date AS purchase_date,
           '' AS transaction_id,
           subs_id,
           a.msisdn,
           int(offer_id) AS price_plan_id,
           brand,
           int(pre_post_flag) pre_post_flag,
           cust_type_desc,
           cust_subtype_desc,
           '' AS customer_sub_segment,
           lac,
           ci,
           lacci_id,
           node_type AS node,
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
           substr(content_id,1,8) AS sigma_business_id,
           substr(pack_id,9,5) AS sigma_rules_id,
           '' AS sku,
           '' AS l1_payu,
           '' AS l2_service_type,
           '' AS l3_allowance_type,
           '' AS l4_product_category,
           '' AS l5_product,
           l1_name AS l1_ias,
           l2_name AS l2_ias,
           l3_name AS l3_ias,
           '' AS commercial_name,
           '' AS channel,
           '' AS pack_validity,
           cast(sum(rev/1.11) AS decimal (38,15)) AS rev_per_usage,
           cast(sum(0) AS decimal (38,15)) AS rev_seized,
           cast(sum(dur) AS int) AS dur,
           cast(sum(trx) AS int) AS trx,
           cast(sum(vol) AS bigint) AS vol,
           null AS cust_id,
           '' AS profile_name,
           '' AS quota_name,
           '' AS service_filter,
           offer AS price_plan_name,
           activation_channel_id AS channel_id,
           '' AS site_id,
           '' AS site_name,
           region_hlr,
           city_hlr,
           {load_date} AS load_date,
           cast (a.event_Date as DATE) AS event_date,
           'WISDOM-NBP' AS source

    FROM  (
    SELECT a.* FROM (
    SELECT a.* FROM (

    SELECT *
    FROM {table_1}
    WHERE event_date = {event_date}
      AND pre_post_flag = '1'
      AND l1_name = 'Digital Services'
      AND lower(l3_name) not like 'phonebook%backup'
      AND SOURCE in ('CHG_GOOD','CHG_REJECT')
    ) a

    INNER JOIN (
    SELECT event_Date,
           lpad(business_id, 8, '0') AS bid
    FROM {table_2}
    WHERE event_date = {event_date}
      AND status_flag = 'Y'
      AND lower(ifrs_flag) = 'true'
    GROUP BY 1,2
    ) c9
    ON a.event_Date = c9.event_Date
      AND substr(pack_id, 1, 8) = c9.bid
    ) a

    LEFT JOIN (
    SELECT sigma_business_id
    FROM {table_3}
    WHERE event_date = {event_date}
      AND SOURCE in ('CHG','SEIZED','UPCC-SEIZED','UPCC_USAGE','PAYU','WISDOM')
    GROUP BY 1
    ) b
    ON substr(pack_id, 1, 8) = b.sigma_business_id
    WHERE b.sigma_business_id is NULL
    ) a

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
df.createOrReplaceTempView("temp_merge_revenue_ifrs_dd_nbp")

# Then use Spark SQL to insert overwrite into the target Hive table
spark.sql("""
INSERT OVERWRITE TABLE testing.merge_revenue_ifrs_dd_poc_tokenized
PARTITION (load_date, event_date, source)
SELECT *
FROM temp_merge_revenue_ifrs_dd_nbp
""")
