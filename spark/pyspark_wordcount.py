"""
Simple PySpark Application which uses Multiple DEX Resources.
Word Count and Read from Mulitple Files.

Run with:
  ./bin/spark-submit $PWD/pyspark_wordcount.py data_file_path template_file_path
"""
from __future__ import print_function

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: pyspark_wordcount.py <"data_file_path"> <"template_file_path">', file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    # Counting the words from <"data_file_path">
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    output = counts.collect()

    # Printing the Template from <"template_file_path">
    template_lines = spark.read.text(sys.argv[2])
    print("{template_lines}".format(template_lines=template_lines), file=sys.stdout)


    # Printing the Word Count Result on Sysout
    for (word, count) in output:
        print("%s: %i" % (word, count))

    print("Word Count Job Has Reached it's End!", file=sys.stdout)
    spark.stop()
