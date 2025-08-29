import os
import pandas as pd

# --- Configuration ---
# Define file paths using the project's directory structure
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(project_dir, "data", "staging")
MASTER_CSV_PATH = os.path.join(project_dir, "data", "ski-data.csv")

def merge_staging_files():
    """
    Merges all CSV files from the staging directory into the master CSV file,
    ensuring no duplicate rows are added.
    """
    print("--- Starting Merge Process ---")

    # 1. Read the existing master data into a pandas DataFrame
    if os.path.exists(MASTER_CSV_PATH):
        print(f"Reading existing data from {os.path.basename(MASTER_CSV_PATH)}...")
        master_df = pd.read_csv(MASTER_CSV_PATH)
    else:
        print(f"Master file not found at {MASTER_CSV_PATH}. A new one will be created.")
        master_df = pd.DataFrame()

    # Create a set of unique identifiers from the master data for quick lookups
    # We'll use a combination of Name, RaceName, and Event to define a unique result
    existing_records = set(tuple(x) for x in master_df[['Name', 'RaceName', 'Event']].values)
    print(f"Found {len(existing_records)} existing unique records.")

    # 2. Scan the staging directory for new files
    if not os.path.exists(STAGING_DIR):
        print(f"Staging directory not found at {STAGING_DIR}. No new files to merge.")
        return

    staging_files = [f for f in os.listdir(STAGING_DIR) if f.endswith('.csv')]
    if not staging_files:
        print("No new files found in the staging directory.")
        return

    print(f"Found {len(staging_files)} new files to process in staging directory.")
    new_rows = []

    # 3. Process each staging file
    for filename in staging_files:
        staging_file_path = os.path.join(STAGING_DIR, filename)
        try:
            staging_df = pd.read_csv(staging_file_path)
            
            for index, row in staging_df.iterrows():
                # Define a unique key for the current row
                record_key = (row['Name'], row['RaceName'], row['Event'])
                
                # If the record is not already in our master list, add it
                if record_key not in existing_records:
                    new_rows.append(row)
                    existing_records.add(record_key) # Add to our set to prevent duplicates within the same run
            
            # After processing, remove the staging file
            os.remove(staging_file_path)
            print(f"Successfully processed and removed {filename}.")

        except Exception as e:
            print(f"Error processing {filename}: {e}. This file will be left in staging.")

    # 4. Append new rows to the master DataFrame and save
    if new_rows:
        print(f"Adding {len(new_rows)} new unique records to the master file.")
        new_rows_df = pd.DataFrame(new_rows)
        master_df = pd.concat([master_df, new_rows_df], ignore_index=True)
        
        # Sort the final CSV for consistency
        master_df.sort_values(by=['Date', 'RaceName', 'Name'], inplace=True)
        
        master_df.to_csv(MASTER_CSV_PATH, index=False)
        print("Master CSV file has been updated.")
    else:
        print("No new unique records to add.")

    print("--- Merge Process Finished ---")

if __name__ == "__main__":
    merge_staging_files()