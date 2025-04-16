import time
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
import os
import csv
import shutil

CSV_INPUT_PATH = "data/benchmark_data_10000000.csv"
results = []

# --- Load Base Data Once ---
df_pd = pd.read_csv(CSV_INPUT_PATH)
df_pl = pl.read_csv(CSV_INPUT_PATH)
spark = SparkSession.builder.appName("Write Benchmark").getOrCreate()
df_sp = spark.read.option("header", "true").csv(CSV_INPUT_PATH, inferSchema=True)

# --- Pandas: CSV ---
start = time.time()
df_pd.to_csv("data/pandas_output.csv", index=False)
end = time.time()
results.append(("pandas_csv", end - start))

# --- Pandas: Parquet ---
start = time.time()
df_pd.to_parquet("data/pandas_output.parquet", index=False)
end = time.time()
results.append(("pandas_parquet", end - start))

# --- Polars: CSV ---
start = time.time()
df_pl.write_csv("data/polars_output.csv")
end = time.time()
results.append(("polars_csv", end - start))

# --- Polars: Parquet ---
start = time.time()
df_pl.write_parquet("data/polars_output.parquet")
end = time.time()
results.append(("polars_parquet", end - start))

# --- PySpark: CSV ---
start = time.time()
df_sp.write.mode("overwrite").option("header", "true").csv("data/spark_output_csv")
end = time.time()
results.append(("pyspark_csv", end - start))

# --- PySpark: Parquet ---
start = time.time()
df_sp.write.mode("overwrite").parquet("data/spark_output_parquet")
end = time.time()
results.append(("pyspark_parquet", end - start))

spark.stop()

# --- Save Results ---
with open("results/write_time.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["library_format", "write_time_seconds"])
    writer.writerows(results)

print("✅ Write benchmark complete. Results saved to results/write_time.csv")

# --- Cleanup generated files/directories ---
paths_to_remove = [
    "data/pandas_output.csv",
    "data/pandas_output.parquet",
    "data/polars_output.csv",
    "data/polars_output.parquet",
    "data/spark_output_csv",
    "data/spark_output_parquet"
]

for path in paths_to_remove:
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

print("🧹 Cleaned up output files after benchmark.")