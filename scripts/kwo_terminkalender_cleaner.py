import os
import pandas as pd
import requests
import hashlib

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KWO_TERMINKALENDER_CSV_PATH = os.path.join(project_dir, "data", "kwo_terminkalender_2025.csv")
PROCESSED_CSV_PATH = os.path.join(project_dir, "data", "kwo_terminkalender_2025_processed.csv")


# Read the CSV file into a pandas DataFrame
# Make sure to use a semicolon ';' as the delimiter if that's what your CSV uses
df = pd.read_csv(KWO_TERMINKALENDER_CSV_PATH, delimiter=';')

# Create a new 'url' column by formatting the 'V-Nr' column
# This creates the URL in the format you requested
df['url'] = df['V-Nr'].apply(lambda x: f'https://www.swiss-ski-kwo.ch/tk/ranglisten/2025/{x}.pdf')

# --- New Duplicate Detection Logic ---

# A dictionary to store hashes of downloaded files to detect duplicates.
# Key: hash, Value: URL
seen_file_hashes = {}
rows_to_keep = []

for index, row in df.iterrows():
    url = row['url']
    try:
        # Download the PDF content
        response = requests.get(url, timeout=10) # Added a timeout for safety
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Calculate the hash of the file content
        file_hash = hashlib.md5(response.content).hexdigest()

        # If we haven't seen this hash before, add it to our dictionary and keep the row
        if file_hash not in seen_file_hashes:
            seen_file_hashes[file_hash] = url
            rows_to_keep.append(index)
            print(f"Keeping: {url} (hash: {file_hash})")
        else:
            print(f"Skipping duplicate: {url} (same content as: {seen_file_hashes[file_hash]})")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}. Skipping this URL.")
        # Decide if you want to keep rows that failed to download. Here, we are dropping them.

# Filter the DataFrame to keep only the unique rows
df_processed = df.loc[rows_to_keep]

# Save the processed DataFrame to a new CSV file
df_processed.to_csv(PROCESSED_CSV_PATH, index=False, sep=';')

print(f"\n--- Processed data saved to {PROCESSED_CSV_PATH} ---")
print(df_processed.head())