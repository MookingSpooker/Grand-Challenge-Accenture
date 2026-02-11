import pandas as pd
import numpy as np
import os

def check_dataset_integrity(file_list):
 
    EXPECTED_COLUMNS = [
        'Country', 'Air Quality Network', 'Air Quality Network Name',
        'Air Quality Station EoI Code', 'Air Quality Station Name',
        'Sampling Point Id', 'Air Pollutant', 'Air Pollutant Description',
        'Data Aggregation Process Id', 'Data Aggregation Process', 'Year',
        'Air Pollution Level', 'Unit Of Air Pollution Level', 'Data Coverage',
        'Verification', 'Air Quality Station Type', 'Air Quality Station Area',
        'Longitude', 'Latitude', 'Altitude', 'City', 'City Code',
        'City Population', 'Source Of Data Flow', 'Calculation Time',
        'Link to raw data (only E1a/validated data from AQ e-Reporting)',
        'Observation Frequency'
    ]

    
    NUMERIC_COLUMNS = [
        'Year', 'Air Pollution Level', 'Data Coverage', 'Verification', 
        'Longitude', 'Latitude', 'Altitude', 'City Population'
    ]

    report = {}

    for file_path in file_list:
        if not os.path.exists(file_path):
            report[file_path] = ["Error: File not found"]
            continue
            
        issues = []
        try:
            
            df_headers = pd.read_csv(file_path, nrows=0)
            actual_columns = list(df_headers.columns)

           
            if actual_columns != EXPECTED_COLUMNS:
                missing = set(EXPECTED_COLUMNS) - set(actual_columns)
                extra = set(actual_columns) - set(EXPECTED_COLUMNS)
                if missing: issues.append(f"Missing columns: {missing}")
                if extra: issues.append(f"Unexpected columns: {extra}")
                if not missing and not extra:
                    issues.append("Column order is inconsistent with the standard schema.")

            
            df = pd.read_csv(file_path)
            
            for col in NUMERIC_COLUMNS:
                if col in df.columns:
                    
                    invalid_mask = pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()
                    error_count = invalid_mask.sum()
                    if error_count > 0:
                        issues.append(f"Format Error: Column '{col}' has {error_count} non-numeric values.")

           
            if 'Year' in df.columns:
                
                try:
                    expected_year = int(os.path.basename(file_path).split('_')[0])
                    wrong_years = df[df['Year'] != expected_year]['Year'].unique()
                    if len(wrong_years) > 0:
                        issues.append(f"Data Consistency: Found years {wrong_years} in a {expected_year} file.")
                except ValueError:
                    pass 

            report[file_path] = issues if issues else ["Integrity Check Passed"]

        except Exception as e:
            report[file_path] = [f"Critical Error: {str(e)}"]

    return report

files_to_check = [
    "2023_NO2_IT.csv", "2023_O3_IT.csv", "2023_PM10_IT.csv", "2023_PM25_IT.csv",
    "2024_NO2_IT.csv", "2024_O3_IT.csv", "2024_PM10_IT.csv", "2024_PM25_IT.csv"
]

validation_results = check_dataset_integrity(files_to_check)

print(f"{'File Name':<20} | Status/Issues")
print("-" * 60)
for file, logs in validation_results.items():
    print(f"{file:<20} | {logs[0]}")
    for log in logs[1:]:
        print(f"{'':<20} | {log}")
