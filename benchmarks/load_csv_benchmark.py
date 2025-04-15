import time
import csv
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession

CSV_PATH = "data/benchmark_data_100000.csv"
results = []

# Benchmark pandas
start = time.time()
df_pandas = pd.read_csv(CSV_PATH)
end = time.time()
results.append(("pandas", end - start))

# Benchmark polars
start = time.time()
df_polars = pl.read_csv(CSV_PATH)
end = time.time()
results.append(("polars", end - start))

spark = SparkSession.builder \
    .appName("CSV Benchmark") \
    .getOrCreate()

# Benchmark Spark
start = time.time()
df_spark = spark.read.option("header", "true").csv(CSV_PATH)
df_spark.count()
end = time.time()
results.append(("pyspark", end - start))

spark.stop()

# Save results
with open("results/load_time.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["library", "load_time_seconds"])
    writer.writerows(results)

print("✅ Benchmark complete. Results saved to results/load_time.csv")
