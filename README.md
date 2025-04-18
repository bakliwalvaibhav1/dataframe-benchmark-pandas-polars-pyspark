# DataFrame Benchmark: pandas vs polars vs pyspark

This project benchmarks the performance of three major Python DataFrame libraries:

- pandas
- polars
- pyspark

It compares their speed and efficiency across real-world operations on datasets ranging from 100K to 10M+ rows.

---

## Goals

- Understand performance tradeoffs between libraries
- Measure speed of common operations
- Observe scalability with increasing data size
- Evaluate suitability for local vs distributed workloads

---

## Benchmarked Operations

| Operation        | Description                                     |
|------------------|-------------------------------------------------|
| Load CSV         | Time to load a CSV into memory                 |
| Filter Rows      | Simple value-based filtering (value1 > 6)      |
| GroupBy          | Aggregation by category with mean of value2    |
| Join             | Left join with a lookup table                  |
| Write            | CSV and Parquet write performance              |

More benchmarks planned: column subset read, null handling, string operations, wide vs long tables.

---

## How to Run Benchmarks

1. Clone the repository
2. Create a virtual environment and install dependencies
3. Generate datasets using:

```bash
python utils/generate_data.py
```

4. Run any benchmark:

```bash
python benchmarks/load_csv_benchmark.py
```

5. Check results/ folder for runtime logs

> Results are not committed to GitHub. Only code is versioned.

---

## Folder Structure

- benchmarks/ → individual benchmark scripts
- utils/ → data generators, helpers
- data/ → auto-generated CSVs (gitignored)
- results/ → output of benchmarks (gitignored)
- notebooks/ → optional explorations
- charts/ → future visualizations
- README.md → this file
