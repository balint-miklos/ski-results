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

# --- System & User Prompt Definitions ---

SYSTEM_INSTRUCTION = """You are an expert data extraction agent specializing in parsing ski race results from documents. Your sole purpose is to extract requested information accurately and format it into clean, machine-readable CSV data.

**Core Capabilities & Rules:**
1.  **Context Awareness:** You understand that contextual information like `RaceName`, `Date`, and `Location` is often located in the page header, and a `Category` (e.g., U12, U14) is often a sub-header for a group of athletes. You must correctly associate this context with each individual result row.
2.  **Accuracy:** Every row of your CSV output must correspond directly to an actual result entry found within the provided document. Do not invent or infer data that is not present.
3.  **Strict Formatting:** You must adhere to the following CSV output format.

**CSV Output Specification:**
-   **Headers:** The very first line of your response must be the CSV header row: `Name,Category,RaceName,Event,Location,Rank,Date`. Do not omit it.
-   **Date Format:** The `Date` column must always be in `YYYY-MM-DD` format.
-   **Special Ranks:** Use `DNS` for 'Did not start' and `DNF` for 'Did not finish' in the `Rank` column.
-   **Missing Data:** If any information for a field is not available in the source document, leave that field blank in the CSV.
-   **Example Row:** `Alessio Miggiano,U12,Grossegg-Rennen,Riesenslalom,Hoch-Ybrig,12,2025-01-25`

**Final Output Constraint:**
-   You must **ONLY** provide the raw CSV data as your final response. Do not include any introductory text, explanations, summaries, or markdown formatting like ` ```csv`.
"""

USER_PROMPT_TEMPLATE = """Analyze the following document and extract the ski race results based on the criteria below.

**Extraction Criteria:**
Extract all results that meet **either** of the following conditions:
1.  The athlete's listed club is {clubs_list}.
2.  The athlete's name appears on the following list:
{athletes_list}

Generate the CSV output according to your system instructions.
"""

# --- API Key and Model Configuration ---
api_key = os.environ.get("GEMINI_API_KEY")
model = None
if api_key:
    genai.configure(api_key=api_key)
    # The system_instruction is passed to the model at initialization.
    model = genai.GenerativeModel(
        'gemini-2.5-flash',
        system_instruction=SYSTEM_INSTRUCTION
    )
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

def build_user_prompt(monitoring_targets):
    """Dynamically builds the user prompt from the template."""
    clubs_to_monitor = monitoring_targets.get('clubs', [])
    athletes_to_monitor = monitoring_targets.get('athletes', [])

    # Format clubs for the prompt, including potential abbreviations.
    club_strings = []
    for club in clubs_to_monitor:
        if club == "Renngruppe Zürcher Oberland":
            club_strings.append("**Renngruppe Zürcher Oberland** (or its abbreviation **RG Zürcher Oberland**)")
        else:
            club_strings.append(f"**{club}**")
    formatted_clubs = ", ".join(club_strings) if club_strings else "any specified clubs"

    # Format athletes as a bulleted list
    formatted_athletes = "\n".join([f"    - {name}" for name in athletes_to_monitor])

    return USER_PROMPT_TEMPLATE.format(
        clubs_list=formatted_clubs,
        athletes_list=formatted_athletes
    )

def print_prompt_for_dry_run(user_prompt, pdf_content):
    """Prints a readable version of the generated prompt for a dry run."""
    print("\n--- DRY RUN: Generated Prompt ---")
    print("\n[SYSTEM INSTRUCTION]")
    print(SYSTEM_INSTRUCTION)
    print("\n[USER PROMPT]")
    print(user_prompt)
    size_in_kb = round(len(pdf_content) / 1024, 2)
    print(f"\n[PDF content ({size_in_kb} kB) would be attached here]")
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
        pdf_file = {"mime_type": "application/pdf", "data": pdf_content}
        print("Download successful.")

        csv_from_ai = ""
        user_prompt = build_user_prompt(monitoring_targets)

        if is_dry_run:
            print("DRY RUN: Skipping Gemini API call.")
            print_prompt_for_dry_run(user_prompt, pdf_content)
            csv_from_ai = "Name,Category,RaceName,Event,Location,Rank,Date\nJohn Doe,U16,Dry Run Race,Slalom,Dry Run Location,1,2025-01-01"
        else:
            if not model:
                print("Error: Live run requested, but Gemini model is not initialized.")
                return False

            print("LIVE RUN: Sending request to Gemini API...")
            # The system instruction is already part of the model configuration.
            # We only need to send the user-specific parts of the prompt.
            prompt_parts = [user_prompt, pdf_file]
            response = model.generate_content(
                prompt_parts,
                generation_config={"response_mime_type": "text/plain"}
            )
            csv_from_ai = response.text
            print("Gemini API call successful.")

        # Clean the response in case it's wrapped in a markdown block
        cleaned_csv = csv_from_ai.strip()
        if cleaned_csv.startswith("```csv"):
            cleaned_csv = cleaned_csv.lstrip("```csv\n")
        elif cleaned_csv.startswith("```"):
            cleaned_csv = cleaned_csv.lstrip("```\n")
        
        if cleaned_csv.endswith("```"):
            cleaned_csv = cleaned_csv.rstrip("\n```")
        
        cleaned_csv = cleaned_csv.strip()

        # Post-process the CSV data to add the ResultUrl column
        lines = cleaned_csv.split('\n')
        csv_output = ""
        if not lines or not lines[0]:
            print("Warning: Received empty or invalid CSV data from AI. Staging file will be empty.")
        else:
            header = lines[0].strip() + ",ResultUrl"
            rows = [header]
            for line in lines[1:]:
                if line.strip():  # Avoid adding empty lines
                    rows.append(line.strip() + f",{url}")
            csv_output = "\n".join(rows) + "\n"

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

