from fastapi import FastAPI, Request
from arq.connections import create_pool
from app.worker import process_cv  # import your worker function
import json

app = FastAPI()

@app.post("/webhook")
async def whatsapp_webhook(request: Request):
    form_data = await request.form()
    message_data = dict(form_data)
    
    # Forward to worker via Redis/ARQ
    redis = await create_pool()
    await redis.enqueue_job("process_cv", message_data)
    
    print("Forwarded message to worker:", message_data)
    
    return {"status": "received"}  # no Twilio response needed
