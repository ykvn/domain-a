import sys
from pyspark.sql.session import SparkSession

conf = SparkConf()

spark = SparkSession.builder \
    .appName("concurrency test") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# get arguments
argv1 = sys.argv[1]

# define periode
msisdn = f"'{argv1}'"

# Run Query
spark.sql(f"""
    select count(*) from (select msisdn, sum(rev_per_usage) rev_per_usage, sum(rev_seized) rev_seized
    from testing.merge_revenue_ifrs_dd_poc_tokenized
    where msisdn = {msisdn}
    group by msisdn) a
    """)

spark.stop()
