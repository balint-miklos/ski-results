import os
import pandas as pd

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KWO_TERMINKALENDER_CSV_PATH = os.path.join(project_dir, "data", "kwo_terminkalender_2025.csv")
CRAWL_TARGETS_PATH = os.path.join(project_dir, "data", "crawl_targets.json")
MONITORING_TARGETS_PATH = os.path.join(project_dir, "data", "monitoring_targets.json")
STAGING_DIR = os.path.join(project_dir, "data", "staging")

# Read the CSV file into a pandas DataFrame
# Make sure to use a semicolon ';' as the delimiter if that's what your CSV uses
df = pd.read_csv(KWO_TERMINKALENDER_CSV_PATH, delimiter=';')

# Create a new 'url' column by formatting the 'V-Nr' column
# This creates the URL in the format you requested
df['url'] = df['V-Nr'].apply(lambda x: f'https://www.swiss-ski-kwo.ch/tk/ranglisten/2025/{x}.pdf')

# You can print the first few rows to see the result
print(df.head())