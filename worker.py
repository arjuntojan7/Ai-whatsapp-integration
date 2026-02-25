# app/worker.py
from arq import create_pool
from arq.connections import RedisSettings
import pdfplumber
import requests
import asyncio
import os
import json
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re

# ----------------------------
# OpenAI client (uses env variable OPENAI_API_KEY)
# ----------------------------
client = OpenAI()

# ----------------------------
# Utility: Download PDF from URL
# ----------------------------
def download_file(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename

# ----------------------------
# Utility: Call OpenAI GPT for structured extraction
# ----------------------------
# replace the old nlp_extract_cv with this version


def nlp_extract_cv(text: str) -> dict:
    """
    Sends the CV text to OpenAI GPT to extract structured details.
    Returns a dict with parsed fields OR a 'raw_output' fallback.
    """
    prompt = f"""
    Extract the following fields from this resume text:

    - Name
    - Email
    - Phone
    - LinkedIn
    - Portfolio/GitHub
    - Skills (list)
    - Education (list)
    - Experience (list)

    Return the result as JSON only, no explanation text.
    Resume text:
    {text}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    content = response.choices[0].message.content
    # First try: direct json.loads
    try:
        extracted = json.loads(content)
        return extracted
    except Exception:
        pass

    # Second try: remove markdown fences and whitespace
    # Remove triple backticks and language markers like ```json
    cleaned = re.sub(r"```(?:json)?\s*", "", content, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.IGNORECASE).strip()

    # Find first "{" and last "}" and try to decode that substring
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = cleaned[start:end+1]
        try:
            extracted = json.loads(candidate)
            return extracted
        except Exception:
            # still failed -> fall through to raw_output fallback
            pass

    # Final fallback: return raw_output so downstream can log/save it
    return {"raw_output": content}

# ----------------------------
# Utility: Store parsed data to Google Sheets
# ----------------------------
# replace store_to_sheets with this improved version
def store_to_sheets(parsed_data):
    """
    Stores structured CV data into Google Sheets cleanly.
    Skills, Education, and Experience are stored as comma-separated strings.
    """
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gclient = gspread.authorize(creds)

        SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
        if not SPREADSHEET_ID:
            sheet = gclient.open("CV_Parsed_Data").sheet1
        else:
            sheet = gclient.open_by_key(SPREADSHEET_ID).sheet1

        # Extract and normalize fields
        def get_key(d, *candidates):
            for k in candidates:
                if k in d:
                    return d[k]
            return ""

        name = get_key(parsed_data, "Name", "name")
        email = get_key(parsed_data, "Email", "email")
        phone = get_key(parsed_data, "Phone", "phone")
        linkedin = get_key(parsed_data, "LinkedIn", "linkedin")
        portfolio = get_key(parsed_data, "Portfolio/GitHub", "portfolio", "github", "Portfolio")

        # Flatten lists to comma-separated strings
        skills = parsed_data.get("Skills", []) or parsed_data.get("skills", [])
        skills_cell = ", ".join(skills) if isinstance(skills, list) else skills

        education = parsed_data.get("Education", []) or parsed_data.get("education", [])
        education_cell = ", ".join(education) if isinstance(education, list) else education

        experience = parsed_data.get("Experience", []) or parsed_data.get("experience", [])
        experience_cell = ", ".join(experience) if isinstance(experience, list) else experience

        file_type = parsed_data.get("file_type", parsed_data.get("fileType", ""))
        raw_output = parsed_data.get("raw_output", "")

        # Build row in consistent order
        row = [
            name,
            email,
            phone,
            skills_cell,
            education_cell,
            experience_cell,
            linkedin,
            portfolio,
            file_type,
            raw_output,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

        print("DEBUG: About to append row:", row)
        sheet.append_row(row)
        print("Data stored in Google Sheet successfully!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error storing to Google Sheets:", repr(e))



def flatten_to_string(value):
    """
    Converts a list, nested list, or JSON-like string to a clean comma-separated string.
    """
    if isinstance(value, list):
        flat = []
        for item in value:
            flat.append(flatten_to_string(item))
        return ", ".join([f for f in flat if f])
    
    # If value looks like a JSON list string, parse it
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                return flatten_to_string(parsed)
            except Exception:
                pass
    return str(value)

def store_to_sheets(parsed_data):
    """
    Stores CV data into a Google Sheet that already has headers.
    Only stores fields that match the headers.
    """
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gclient = gspread.authorize(creds)

        # Open your specific sheet by ID
        SPREADSHEET_ID = "1YXb8l5p20Jfe6Mh4I7OIm7jl7e8fMyKkbBkwTkoZE-M"
        sheet = gclient.open_by_key(SPREADSHEET_ID).sheet1

        # Read the header row
        headers = sheet.row_values(1)  # ['name', 'email', 'phone', 'skills', 'education']

        # Helper to flatten nested lists or JSON-like strings
        def flatten_to_string(value):
            if isinstance(value, list):
                flat = []
                for item in value:
                    flat.append(flatten_to_string(item))
                return ", ".join([f for f in flat if f])
            if isinstance(value, str):
                value = value.strip()
                if value.startswith("[") and value.endswith("]"):
                    try:
                        parsed = json.loads(value)
                        return flatten_to_string(parsed)
                    except Exception:
                        pass
            return str(value)

        # Map parsed_data to headers
        row = []
        for head in headers:
            head_lower = head.lower().strip()
            if head_lower == "name":
                row.append(parsed_data.get("Name", parsed_data.get("name", "")))
            elif head_lower == "email":
                row.append(parsed_data.get("Email", parsed_data.get("email", "")))
            elif head_lower == "phone":
                row.append(parsed_data.get("Phone", parsed_data.get("phone", "")))
            elif head_lower in ["linkedin", "linked_in"]:
                row.append(parsed_data.get("LinkedIn", parsed_data.get("linkedin", "")))
            elif head_lower in ["portfolio/github", "portfolio", "github"]:
                row.append(parsed_data.get("Portfolio/GitHub", parsed_data.get("portfolio", parsed_data.get("github", ""))))
            elif head_lower == "skills":
                row.append(flatten_to_string(parsed_data.get("Skills", parsed_data.get("skills", ""))))
            elif head_lower == "education":
                row.append(flatten_to_string(parsed_data.get("Education", parsed_data.get("education", ""))))
            elif head_lower == "experience":
                row.append(flatten_to_string(parsed_data.get("Experience", parsed_data.get("experience", ""))))
            elif head_lower == "timestamp":
                from datetime import datetime
                row.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                row.append("")

        print("DEBUG: About to append row:", row)
        sheet.append_row(row)
        print("Data stored in Google Sheet successfully!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error storing to Google Sheets:", repr(e))


# ----------------------------
# Worker function: Process message
# ----------------------------
async def process_cv(ctx, message_data: dict):
    print(" Processing message:", message_data)

    num_media = int(message_data.get('NumMedia', 0))
    extracted_info = {}

    if num_media > 0:
        media_type = message_data.get('MediaContentType0')
        if media_type == "application/pdf":
            media_url = message_data.get('MediaUrl0')
            file_path = download_file(media_url, "incoming.pdf")

            text = ""
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"

            print(" Parsed PDF text snippet:", text[:500])
            extracted_info = nlp_extract_cv(text)
            extracted_info["file_type"] = "pdf"

    else:
        # Text message (CV pasted directly)
        text = message_data.get('Body', '')
        print(" Received text message snippet:", text[:500])
        extracted_info = nlp_extract_cv(text)
        extracted_info["file_type"] = "text"

    # Display structured result
    print(" Extracted structured details:", extracted_info)

    #  Save to Google Sheets
    try:
        store_to_sheets(extracted_info)
    except Exception as e:
        print(" Error saving to Google Sheet:", e)

    # Simulate async completion
    await asyncio.sleep(1)
    return {"status": "done"}


# ----------------------------
# ARQ Worker Settings
# ----------------------------
class WorkerSettings:
    redis_settings = RedisSettings()
    functions = [process_cv]

# ----------------------------
# Utility: Download PDF from Twilio (authenticated)
# ----------------------------
import os
from requests.auth import HTTPBasicAuth

def download_file(url, filename):
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")

    response = requests.get(url, auth=HTTPBasicAuth(account_sid, auth_token))
    if response.status_code != 200:
        raise Exception(f"Failed to download file from Twilio. Status: {response.status_code}")
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename
