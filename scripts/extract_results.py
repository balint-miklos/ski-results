import requests
import os
from datetime import datetime
import google.generativeai as genai

# URL of the PDF file to be processed
url = "https://www.swiss-ski-kwo.ch/tk/ranglisten/2025/1396.pdf"

# Define file paths
# The script's directory for temporary files
script_dir = os.path.dirname(os.path.abspath(__file__))
# The parent directory, where ski-data.csv is located
parent_dir = os.path.dirname(script_dir)

pdf_path = os.path.join(script_dir, "1396.pdf")
txt_path = os.path.join(script_dir, "1396_log.txt")
# Correct the path to look one folder up for the CSV file
csv_path = os.path.join(parent_dir, "ski-data.csv")

# --- API Key Configuration ---
# Retrieve the API key from environment variables for security
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is not set.")

# Configure the Gemini API with the provided key
genai.configure(api_key=api_key)

# Initialize the Generative Model
# Using gemini-1.5-flash as it's a powerful and efficient model for this task
model = genai.GenerativeModel('gemini-2.5-flash')

# --- File Handling ---
# Download the PDF from the specified URL
print(f"Downloading PDF from {url}...")
response = requests.get(url)
response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
with open(pdf_path, "wb") as f:
    f.write(response.content)
print(f"PDF saved to {pdf_path}")

# Get file size in kB for logging
file_size_kb = round(os.path.getsize(pdf_path) / 1024, 2)

# Read the downloaded PDF content in binary mode
with open(pdf_path, "rb") as pdf_file:
    pdf_content = pdf_file.read()

# Read the existing CSV file content if it exists, otherwise use an empty string
csv_content = ""
if os.path.exists(csv_path):
    with open(csv_path, "r", encoding='utf-8') as csv_file:
        csv_content = csv_file.read()
else:
    print(f"CSV file not found at {csv_path}. A new one will be created based on the model's output.")


# --- Prompt Preparation for Gemini ---
# The generate_content method expects a list of parts (strings or file data dicts).
# This is a more direct and SDK-friendly way to build the prompt.
prompt_parts = [
    "You are an expert in data extraction from PDF files.",
    "Analyze the attached PDF, which contains ski race results.",
    "Identify all athletes from the club 'Renngruppe Zürcher Oberland'.",
    "Extract their results and add them as new rows to the following existing CSV data.",
    "Do not remove any existing results from the CSV.",
    "The final output should only be the complete, updated CSV data and nothing else.",
    # For file data, provide a dict with mime_type and the raw data bytes.
    # The SDK handles the necessary encoding.
    {
        "mime_type": "application/pdf",
        "data": pdf_content
    },
    "\n--- Existing CSV Data ---\n",
    csv_content
]

# --- AI Model Interaction ---
# Generate the new content by sending the parts list to the model
print("Sending request to Gemini API...")
response = model.generate_content(prompt_parts)

# --- Output and Cleanup ---
# Print the raw text response from the model
print("\n--- Gemini Response ---\n")
print(response.text)

# You would typically add code here to save the response.text to your CSV file
# For example:
# with open(csv_path, "w", encoding='utf-8') as f:
#     f.write(response.text)
# print(f"\nUpdated data saved to {csv_path}")

# Delete the temporary PDF file
os.remove(pdf_path)
print(f"\nTemporary file {pdf_path} has been deleted.")
