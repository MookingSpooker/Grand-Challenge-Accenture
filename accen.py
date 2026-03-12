import pandas as pd
import os

files= [
    "2023_NO2_IT.csv", "2023_O3_IT.csv", "2023_PM10_IT.csv", "2023_PM25_IT.csv",
    "2024_NO2_IT.csv", "2024_O3_IT.csv", "2024_PM10_IT.csv", "2024_PM25_IT.csv"
]

all_data = []
for f in files:
    df = pd.read_csv(f)

    mask = df['Data Aggregation Process'].str.contains('Annual mean|1 calendar year', case=False, na=False)
    df_filtered = df[mask].copy()

    if df_filtered.empty:
        most_common = df['Data Aggregation Process'].value_counts().index[0]
        df_filtered =df[df['Data Aggregation Process'] == most_common].copy()

cols_to_keep = [
    'Air Pollutant', 'Year', 'Air Quality Station EoI Code', 'Air Quality Station Name',
    'City', 'Longitude', 'Latitude', 'Air Pollution Level', 'Unit of Air Pollution Level',
    'Air Quality Station Type', 'Air Quality Station Area', 'City'
]

df_filtered = df_filtered[[c for c in cols_to_keep if c in df_filtered.columns]]

all_data.append(df_filtered)

heatmap_df = pd.concat(all_data, ignore_index=True)
heatmap_df.to_csv("heatmap_data_IT.csv", index=False)






