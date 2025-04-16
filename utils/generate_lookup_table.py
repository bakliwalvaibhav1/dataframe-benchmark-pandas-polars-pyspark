import pandas as pd

categories = ['A', 'B', 'C']
labels = ['Alpha', 'Beta', 'Gamma']

df = pd.DataFrame({
    "category": categories,
    "label": labels
})

df.to_csv("data/lookup.csv", index=False)
print("✅ Lookup table saved to data/lookup.csv")
