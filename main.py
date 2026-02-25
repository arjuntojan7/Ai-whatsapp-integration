from fastapi import FastAPI, Request
import requests
import pdfplumber

app = FastAPI()

def download_file(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename

@app.post("/webhook")
async def whatsapp_webhook(request: Request):
    form_data = await request.form()
    num_media = int(form_data.get('NumMedia', 0))
    
    if num_media > 0:
        media_url = form_data.get('MediaUrl0')
        media_type = form_data.get('MediaContentType0')
        
        if media_type == "application/pdf":
            file_path = download_file(media_url, "incoming.pdf")
            text = ""
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() + "\n"
            
            # Store 'text' in Google Sheets / Drive here
            print("Parsed PDF text:", text[:500])
    
    else:
        msg_body = form_data.get('Body', '')
        # Store 'msg_body' in Google Sheets / Drive here
        print("Received text message:", msg_body)

    # No Twilio response is sent
    return {"status": "success"}  # Twilio just needs 200 OK
