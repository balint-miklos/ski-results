import requests
import os
from datetime import datetime
import google.generativeai as genai

url = "https://www.swiss-ski-kwo.ch/tk/ranglisten/2025/1396.pdf"
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pdf_path = os.path.join(repo_root, "1396.pdf")
txt_path = os.path.join(repo_root, "1396.txt")

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is not set.")

# Configure the Gemini API
genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-2.5-flash')

# Download the PDF
response = requests.get(url)
response.raise_for_status()
with open(pdf_path, "wb") as f:
    f.write(response.content)

# Get file size in kB
file_size_kb = round(os.path.getsize(pdf_path) / 1024, 2)

# Write the info text file
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(txt_path, "w") as f:
    f.write(f"I downloaded on {now} the file {url} and its size is {file_size_kb} kB.\n")

# Delete the PDF file
os.remove(pdf_path)

# Generate a dad joke using Gemini
response = model.generate_content("hi, write me a short dad joke")
print("Gemini dad joke:", response.text)
