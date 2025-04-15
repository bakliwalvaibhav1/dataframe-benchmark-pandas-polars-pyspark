import time
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv

CSV_PATH = "data/benchmark_data_10000000.csv"  # adjust as needed
results = []

# --- Pandas ---
df_pandas = pd.read_csv(CSV_PATH)
start = time.time()
filtered_pandas = df_pandas[df_pandas["value1"] > 6.0]
end = time.time()
results.append(("pandas", end - start))

# --- Polars ---
df_polars = pl.read_csv(CSV_PATH)
start = time.time()
filtered_polars = df_polars.filter(pl.col("value1") > 6.0)
end = time.time()
results.append(("polars", end - start))

# --- PySpark ---
spark = SparkSession.builder.appName("Filter Benchmark").getOrCreate()
df_spark = spark.read.option("header", "true").csv(CSV_PATH, inferSchema=True)
start = time.time()
filtered_spark = df_spark.filter(col("value1") > 6.0)
filtered_spark.count()  # trigger execution
end = time.time()
results.append(("pyspark", end - start))
spark.stop()

# --- Save Results ---
with open("results/filter_time.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["library", "filter_time_seconds"])
    writer.writerows(results)

print("✅ Filter benchmark complete. Results saved to results/filter_time.csv")
