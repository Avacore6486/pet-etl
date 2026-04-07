import pandas as pd

data = {
    "name": ["Аня", "Боря", "Вася", "Галя", "Дима"],
    "age": [25, 17, 30, 22, 35],
    "city": ["Москва", "Питер", "Москва", "Казань", "Питер"],
    "salary": [80000, 50000, 120000, 65000, 95000],
    "department": ["IT", "HR", "IT", "Finance", "IT"],
}

df = pd.DataFrame(data)
df.to_parquet("sample_data.parquet", index=False)
print(df)
