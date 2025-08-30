import os
import pandas as pd

# --- Configuration ---
# Define file paths using the project's directory structure
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(project_dir, "data", "staging")
MASTER_CSV_PATH = os.path.join(project_dir, "data", "ski-data.csv")

def merge_staging_files():
    """
    Reads all CSV files from the staging directory, merges them, removes duplicates,
    and overwrites the master CSV file with the result.
    """
    print("--- Starting Simplified Merge Process ---")

    # 1. Scan the staging directory for new files
    if not os.path.exists(STAGING_DIR):
        print(f"Staging directory not found at {STAGING_DIR}. No files to merge.")
        # Create an empty master file if it doesn't exist
        if not os.path.exists(MASTER_CSV_PATH):
             pd.DataFrame().to_csv(MASTER_CSV_PATH, index=False)
        return

    staging_files = [f for f in os.listdir(STAGING_DIR) if f.endswith('.csv')]
    if not staging_files:
        print("No CSV files found in the staging directory. The master file will be cleared.")
        # Overwrite with an empty DataFrame to clear it
        pd.DataFrame().to_csv(MASTER_CSV_PATH, index=False)
        return

    print(f"Found {len(staging_files)} CSV files to process in staging directory.")
    
    # 2. Read all staging files into a list of DataFrames
    all_dataframes = []
    for filename in staging_files:
        staging_file_path = os.path.join(STAGING_DIR, filename)
        try:
            df = pd.read_csv(staging_file_path)
            all_dataframes.append(df)
        except Exception as e:
            print(f"Warning: Could not read or process {filename}: {e}. Skipping this file.")

    if not all_dataframes:
        print("No valid data could be read from staging files. Master file will be cleared.")
        pd.DataFrame().to_csv(MASTER_CSV_PATH, index=False)
        print("--- Merge Process Finished ---")
        return

    # 3. Concatenate all dataframes into a single one
    print("Merging all dataframes...")
    master_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 4. Remove duplicate rows to ensure data integrity
    # A unique result is defined by a combination of Name, RaceName, and Event
    unique_columns = ['Name', 'RaceName', 'Event']
    # Ensure columns exist before trying to drop duplicates based on them
    if all(col in master_df.columns for col in unique_columns):
        initial_rows = len(master_df)
        master_df.drop_duplicates(subset=unique_columns, keep='last', inplace=True)
        print(f"Removed {initial_rows - len(master_df)} duplicate records.")
    else:
        print("Warning: One or more unique identifier columns (Name, RaceName, Event) not found. Skipping duplicate removal.")

    # 5. Sort the final CSV for consistency
    if 'Date' in master_df.columns and 'RaceName' in master_df.columns and 'Name' in master_df.columns:
        master_df.sort_values(by=['Date', 'RaceName', 'Name'], inplace=True)
        print("Sorting data by Date, RaceName, and Name.")
    
    # 6. Overwrite the master CSV file
    master_df.to_csv(MASTER_CSV_PATH, index=False)
    print(f"Successfully overwrote {os.path.basename(MASTER_CSV_PATH)} with {len(master_df)} unique records.")

    print("--- Merge Process Finished ---")

if __name__ == "__main__":
    merge_staging_files()
