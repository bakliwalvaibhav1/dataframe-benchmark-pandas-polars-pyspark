import random
import pandas as pd
from datetime import datetime, timedelta

ROW_COUNT = 10_000_000_000
START_DATE = datetime(2020, 1, 1)
CATEGORIES = ['A', 'B', 'C']

rows = []

for i in range(ROW_COUNT):
    row = {
        "id": i + 1,
        "category": random.choice(CATEGORIES),
        "value1": round(random.uniform(3, 9), 2),
        "value2": random.randint(20, 60),
        "timestamp": (START_DATE + timedelta(days=i % 3650)).strftime("%Y-%m-%d")
    }
    rows.append(row)

df = pd.DataFrame(rows)
df.to_csv(f"data/benchmark_data_{ROW_COUNT}.csv", index=False)
