from pyspark.sql import SparkSession

# 1. Create a SparkSession
# This is the entry point to any PySpark application.
spark = SparkSession.builder \
    .appName("SimplePySparkExample") \
    .getOrCreate()

# 2. Create a sample DataFrame
# You can load data from various sources (CSV, Parquet, etc.),
# but for a simple example, we'll create one from a list.
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("David", 4)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# 3. Perform a simple transformation (e.g., filter data)
# This example filters rows where the ID is greater than 2.
filtered_df = df.filter(df["ID"] > 2)

# 4. Display the results
filtered_df.show()

# 5. Stop the SparkSession
# It's good practice to stop the SparkSession when you're done.
spark.stop()
