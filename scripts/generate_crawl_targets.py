import os
import pandas as pd
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

# --- Configuration ---
# Define file paths using the project's directory structure
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_CSV_PATH = os.path.join(project_dir, "data", "kwo_terminkalender_2025_processed.csv")
CRAWL_TARGETS_JSON_PATH = os.path.join(project_dir, "data", "crawl_targets_2025.json")

def create_crawl_targets():
    """
    Reads the processed event calendar CSV and generates a JSON file
    of crawl targets based on the defined schema.
    """
    print(f"--- Starting Crawl Target Generation ---")
    print(f"Reading processed data from: {os.path.basename(PROCESSED_CSV_PATH)}")

    try:
        # Read the processed CSV file
        df = pd.read_csv(PROCESSED_CSV_PATH, delimiter=';')
    except FileNotFoundError:
        print(f"Error: Processed file not found at {PROCESSED_CSV_PATH}. Please run the crawl_target_creator.py script first.")
        return

    crawl_targets = []
    now_utc_iso = datetime.now(timezone.utc).isoformat()

    # Iterate over each row in the DataFrame to build the target list
    for index, row in df.iterrows():
        try:
            # Ensure 'Datum' is a string and parse it
            event_date_str = str(row['Datum'])
            event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
            
            # Define the crawl window
            valid_from = event_date.replace(tzinfo=timezone.utc)
            valid_until = (valid_from + relativedelta(years=1)).replace(second=59, minute=59, hour=23)


            # Create the unique ID for the target
            target_id = f"kwo2025-{row['V-Nr']}"

            # Assemble the crawl target object
            target = {
                "id": target_id,
                "url": row['url'],
                "status": "queued",
                "event": {
                    "startDate": event_date.strftime('%Y-%m-%d'),
                    "endDate": event_date.strftime('%Y-%m-%d')
                },
                "crawlPolicy": {
                    "validFrom": valid_from.isoformat(),
                    "validUntil": valid_until.isoformat()
                },
                "tracking": {
                    "createdAt": now_utc_iso,
                    "updatedAt": now_utc_iso,
                    "attemptCount": 0,
                    "lastAttemptAt": None,
                    "succeededAt": None
                }
            }
            crawl_targets.append(target)
        except (ValueError, TypeError) as e:
            print(f"Warning: Could not process row {index}. Invalid date format or value. Error: {e}")
            continue

    # Save the list of targets to a JSON file
    print(f"Generated {len(crawl_targets)} crawl targets.")
    with open(CRAWL_TARGETS_JSON_PATH, 'w') as f:
        json.dump(crawl_targets, f, indent=2)

    print(f"Successfully saved crawl targets to {os.path.basename(CRAWL_TARGETS_JSON_PATH)}")
    print(f"--- Crawl Target Generation Finished ---")


if __name__ == "__main__":
    create_crawl_targets()
    
