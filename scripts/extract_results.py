import os
import json
import requests
import argparse
import google.generativeai as genai
from datetime import datetime, timezone

# --- Configuration ---
# Define file paths using the project's directory structure
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRAWL_TARGETS_PATH = os.path.join(project_dir, "data", "crawl_targets.json")
MONITORING_TARGETS_PATH = os.path.join(project_dir, "data", "monitoring_targets.json")
STAGING_DIR = os.path.join(project_dir, "data", "staging")

# --- API Key Configuration ---
api_key = os.environ.get("GEMINI_API_KEY")
model = None
if api_key:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    print("Warning: GEMINI_API_KEY not set. Script can only run in dry-run mode.")

def load_json_file(file_path):
    """Loads a generic JSON file and returns its content."""
    print(f"Loading data from {os.path.basename(file_path)}...")
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}.")
        return None

def save_crawl_targets(targets):
    """Saves the updated list of targets back to the JSON file."""
    print(f"Saving updated targets to {CRAWL_TARGETS_PATH}...")
    with open(CRAWL_TARGETS_PATH, 'w') as f:
        json.dump(targets, f, indent=2)
    print("Save successful.")

def build_prompt(pdf_content, monitoring_targets):
    """Dynamically builds the prompt for the Gemini API call."""
    clubs_to_monitor = monitoring_targets.get('clubs', [])
    athletes_to_monitor = monitoring_targets.get('athletes', [])
    
    prompt_parts = [
        "You are an expert in data extraction from PDF files.",
        "Analyze the attached PDF, which contains ski race results.",
        "Extract all results for the following clubs and athletes."
    ]

    if clubs_to_monitor:
        prompt_parts.append(f"Clubs to extract: {', '.join(clubs_to_monitor)}")
    
    if athletes_to_monitor:
        prompt_parts.append("Athletes to extract:")
        for athlete_name in athletes_to_monitor:
            prompt_parts.append(f"- {athlete_name}")

    prompt_parts.extend([
        "\nFormat the output as a CSV with these exact headers: Name,Category,RaceName,Event,Rank,Date",
        "The final output should ONLY be the CSV data and nothing else (no introductory text or markdown).",
        {"mime_type": "application/pdf", "data": pdf_content}
    ])
    
    return prompt_parts

def print_prompt_for_dry_run(prompt_parts):
    """Prints a readable version of the generated prompt for a dry run."""
    print("\n--- DRY RUN: Generated Prompt ---")
    for part in prompt_parts:
        if isinstance(part, str):
            print(part)
        elif isinstance(part, dict) and 'data' in part:
            size_in_kb = round(len(part['data']) / 1024, 2)
            print(f"[PDF content of type {part['mime_type']} ({size_in_kb} kB) would be attached here]")
    print("---------------------------------\n")

def process_target(target, monitoring_targets, is_dry_run=True):
    """
    Downloads, processes a single crawl target, and saves the result to the staging area.
    Returns True on success and False on failure.
    """
    target_id = target.get("id")
    url = target.get("url")
    if not all([target_id, url]):
        print(f"Skipping invalid target: {target}")
        return False

    print(f"\n--- Processing Target ID: {target_id} ---")
    try:
        print(f"Downloading PDF from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        pdf_content = response.content
        print("Download successful.")

        csv_output = ""
        if is_dry_run:
            print("DRY RUN: Skipping Gemini API call.")
            prompt_parts = build_prompt(pdf_content, monitoring_targets)
            print_prompt_for_dry_run(prompt_parts)
            csv_output = "Name,Category,RaceName,Event,Rank,Date\nJohn Doe,U16,Dry Run Race,Slalom,1,2025-01-01\n"
        else:
            if not model:
                print("Error: Live run requested, but Gemini model is not initialized.")
                return False
            prompt_parts = build_prompt(pdf_content, monitoring_targets)
            print("LIVE RUN: Sending request to Gemini API...")
            response = model.generate_content(prompt_parts)
            csv_output = response.text
            print("Gemini API call successful.")

        os.makedirs(STAGING_DIR, exist_ok=True)
        staging_file_path = os.path.join(STAGING_DIR, f"{target_id}.csv")
        with open(staging_file_path, "w", encoding='utf-8') as f:
            f.write(csv_output)
        print(f"Successfully saved results to {staging_file_path}")
        return True

    except Exception as e:
        print(f"An error occurred while processing {target_id}: {e}")
        return False

def main():
    """Main function to parse arguments and run the crawler."""
    parser = argparse.ArgumentParser(description="Extract ski race results from PDFs.")
    parser.add_argument(
        '--live-run',
        action='store_true',
        help="Run in live mode, making actual calls to the Gemini API. Default is a dry run."
    )
    args = parser.parse_args()
    is_dry_run = not args.live_run
    
    mode = "LIVE" if not is_dry_run else "DRY-RUN"
    print(f"--- Running in {mode} mode. ---")

    crawl_targets = load_json_file(CRAWL_TARGETS_PATH)
    monitoring_targets = load_json_file(MONITORING_TARGETS_PATH)

    if not crawl_targets or not monitoring_targets:
        print("Could not load necessary target files. Exiting.")
        return
    
    now_utc = datetime.now(timezone.utc)
    
    for target in crawl_targets:
        target_id = target.get("id")
        status = target.get("status")
        policy = target.get("crawlPolicy", {})
        
        if status not in ['queued', 'failed']:
            print(f"\nSkipping target {target_id}: status is '{status}'.")
            continue
            
        valid_from_str = policy.get('validFrom')
        valid_until_str = policy.get('validUntil')
        
        if valid_from_str and valid_until_str:
            valid_from = datetime.fromisoformat(valid_from_str)
            valid_until = datetime.fromisoformat(valid_until_str)
            
            if not (valid_from <= now_utc <= valid_until):
                print(f"\nSkipping target {target_id}: current time is outside the valid crawl window.")
                continue

        success = process_target(target, monitoring_targets, is_dry_run)

        if not is_dry_run:
            now_iso = now_utc.isoformat()
            target['tracking']['lastAttemptAt'] = now_iso
            target['tracking']['updatedAt'] = now_iso
            target['tracking']['attemptCount'] = target.get('tracking', {}).get('attemptCount', 0) + 1
            if success:
                target['status'] = 'processed'
                target['tracking']['succeededAt'] = now_iso
            else:
                target['status'] = 'failed'
    
    if not is_dry_run:
        save_crawl_targets(crawl_targets)
    
    print("\n--- Crawl process finished. ---")

if __name__ == "__main__":
    main()