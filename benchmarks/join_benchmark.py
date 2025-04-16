import time
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv

MAIN_PATH = "data/benchmark_data_10000000.csv"
LOOKUP_PATH = "data/lookup.csv"
results = []

# --- Pandas ---
df_main_pd = pd.read_csv(MAIN_PATH)
df_lookup_pd = pd.read_csv(LOOKUP_PATH)
start = time.time()
joined_pd = df_main_pd.merge(df_lookup_pd, on="category", how="left")
end = time.time()
results.append(("pandas", end - start))

# --- Polars ---
df_main_pl = pl.read_csv(MAIN_PATH)
df_lookup_pl = pl.read_csv(LOOKUP_PATH)
start = time.time()
joined_pl = df_main_pl.join(df_lookup_pl, on="category", how="left")
end = time.time()
results.append(("polars", end - start))

# --- PySpark ---
spark = SparkSession.builder.appName("Join Benchmark").getOrCreate()
df_main_sp = spark.read.option("header", "true").csv(MAIN_PATH, inferSchema=True)
df_lookup_sp = spark.read.option("header", "true").csv(LOOKUP_PATH, inferSchema=True)
start = time.time()
joined_sp = df_main_sp.join(df_lookup_sp, on="category", how="left")
joined_sp.count()  # force execution
end = time.time()
results.append(("pyspark", end - start))
spark.stop()

# --- Save Results ---
with open("results/join_time.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["library", "join_time_seconds"])
    writer.writerows(results)

print("✅ Join benchmark complete. Results saved to results/join_time.csv")
