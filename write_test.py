# test_write.py
import os, json
from google.oauth2 import service_account
import gspread
import sys

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

print("SPREADSHEET_ID:", SPREADS_ID := SPREADSHEET_ID)
print("CREDS_PATH:", CREDS_PATH)

if not SPREADSHEET_ID or not CREDS_PATH:
    print("ERROR: SPREADSHEET_ID or GOOGLE_APPLICATION_CREDENTIALS not set")
    sys.exit(1)

# Load service account credentials and print the service account email
try:
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    print("Service account email:", creds.service_account_email)
except Exception as e:
    print("Failed to load creds file:", e)
    raise

# Try to open sheet and append a test row
try:
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.sheet1
    ws.append_row(["TEST_NAME", "test@example.com", "9999999999", "TEST_SKILLS", "TEST_EDU", "TEST_EXP", "TEST_FILETYPE", "TEST_TS"])
    print("Append succeeded. Last row:", ws.get_all_values()[-1])
except Exception as e:
    # print full exception for debugging
    import traceback
    traceback.print_exc()
    print("Failed to open or write to the spreadsheet:", repr(e))
