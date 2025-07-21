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
    .appName("revenue_pi_dd_poc_tokenized_merge") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Add these settings immediately after SparkSession creation
spark.sql("SET hive.exec.dynamic.partition = true")
spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

# def process_data(spark, env):
#     hc = HiveContext(spark)

#define table
table_source = "usecase_1.revenue_pi_dd_poc_tokenized"
table_target = "testing.revenue_pi_dd_poc_tokenized"

#define periode
# event_date = f'{env["table_1"]["filter_d2"]}'
# load_date  = f'{env["table_1"]["filter_d0"]}'
# month_date = f'{env["table_2"]["filter_month"]}'

event_date = "'2025-04-01'"
load_date = "'2025-04-01'"
month_date = "'2025-03-01'"

# print(f"""run for event_date={event_date} and load_date={load_date}""")

# Refresh Source Table
spark.sql(f"""
    REFRESH TABLE {table_source}
    """)

# Refresh Target Table
spark.sql(f"""
    REFRESH TABLE {table_target}
    """)

QUERY = f"""
    INSERT OVERWRITE TABLE {table_target}
    PARTITION (expiry_date)
    SELECT * FROM {table_source}
    """

# Run query
spark.sql(QUERY)

# Truncate source table
spark.sql(f"""
    TRUNCATE TABLE {table_source}
    """
)

# Drop source table
#spark.sql(f"""
#    DROP TABLE IF EXISTS {table_source}
#    """
#)
