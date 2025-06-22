import pandas as pd

list_a = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
list_b = [{"id": 1, "age": 30}, {"id": 2, "age": 25}]

df_a = pd.DataFrame(list_a)
df_b = pd.DataFrame(list_b)

merged = pd.merge(df_a, df_b, on="id", how="inner")
result = merged.to_dict(orient="records")

x = list(range(1, 3))
y = dict(zip(list_a, x))

print(y)
