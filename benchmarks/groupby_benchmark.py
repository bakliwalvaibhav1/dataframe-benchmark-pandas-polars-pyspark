import time
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import csv

CSV_PATH = "data/benchmark_data_10000000.csv"
results = []

# --- Pandas ---
df_pandas = pd.read_csv(CSV_PATH)
start = time.time()
grouped_pandas = df_pandas.groupby("category")["value2"].mean()
end = time.time()
results.append(("pandas", end - start))

# --- Polars ---
df_polars = pl.read_csv(CSV_PATH)
start = time.time()
grouped_polars = df_polars.group_by("category").agg(pl.col("value2").mean())
end = time.time()
results.append(("polars", end - start))

# --- PySpark ---
spark = SparkSession.builder.appName("GroupBy Benchmark").getOrCreate()
df_spark = spark.read.option("header", "true").csv(CSV_PATH, inferSchema=True)
start = time.time()
grouped_spark = df_spark.groupBy("category").agg(avg(col("value2")))
grouped_spark.count()  # Trigger execution
end = time.time()
results.append(("pyspark", end - start))
spark.stop()

# --- Save Results ---
with open("results/groupby_time.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["library", "groupby_time_seconds"])
    writer.writerows(results)

print("✅ GroupBy benchmark complete. Results saved to results/groupby_time.csv")
