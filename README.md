Tech Stack: Python · FastAPI · OpenAI GPT-4o-mini · Twilio · Redis/ARQ · Google Sheets API · pdfplumber

Built an automated pipeline where candidates send their CV (PDF or text) via WhatsApp, and the system uses GPT-4o-mini to extract structured data (Name, Email, Phone, Skills, Education, Experience) and stores it in Google Sheets in real time.

Developed a FastAPI webhook integrated with Twilio's WhatsApp API for receiving messages and PDF attachments
Implemented async task processing with Redis/ARQ to decouple ingestion from AI processing
Automated structured information extraction from unstructured resumes using OpenAI, with robust JSON parsing fallbacks
Integrated Google Sheets API for real-time candidate database updates
Impact: Eliminated manual resume screening — recruiters get a live, structured spreadsheet of candidates by simply sharing a WhatsApp number.
