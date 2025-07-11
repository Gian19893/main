import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Request, Body
from typing import Optional, Dict
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import mimetypes
from pydantic import BaseModel
import firebase_admin
from firebase_admin import credentials, messaging
import json  # 👈 aggiunto per parsing della chiave

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)

# 🔧 Configurazione Firebase modificata per usare variabile d'ambiente
firebase_key_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not firebase_key_json:
    raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable")

firebase_creds = json.loads(firebase_key_json)
cred = credentials.Certificate(firebase_creds)
firebase_admin.initialize_app(cred)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

db = {}
device_registrations: Dict[str, str] = {}
upload_lock = asyncio.Lock()
upload_queue = asyncio.Queue()

class FcmTokenRequest(BaseModel):
    token: str
    device_id: str

class NotificationRequest(BaseModel):
    id: str
    text: str
    file_type: Optional[str] = None

async def cleanup_old_entries():
    while True:
        try:
            now = datetime.now()
            ten_minutes_ago = now - timedelta(minutes=120)
            to_delete = []
            for id, entry in db.items():
                if "created_at" not in entry:
                    entry["created_at"] = time.time()
                    continue
                if datetime.fromtimestamp(entry["created_at"]) < ten_minutes_ago:
                    to_delete.append(id)
            for id in to_delete:
                entry = db.pop(id, None)
                if entry and "file_path" in entry and entry["file_path"]:
                    try:
                        os.remove(entry["file_path"])
                        logger.info(f"Deleted expired file: {entry['file_path']}")
                    except OSError as e:
                        logger.error(f"Error deleting file {entry['file_path']}: {e}")
            logger.debug(f"Cleanup completed. Current entries: {len(db)}, deleted: {len(to_delete)}")
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}", exc_info=True)
        finally:
            await asyncio.sleep(60)

async def process_upload_job(job):
    id_str, text, file_content, filename, content_type, frequency, scanner_device_id = job
    logger.debug(f"Processing upload job: id={id_str}, device_id={scanner_device_id}")

    async with upload_lock:
        file_path = None
        file_type = None

        if file_content:
            file_type = content_type
            ext = mimetypes.guess_extension(file_type) or os.path.splitext(filename)[1] or ".dat"
            file_path = os.path.join(UPLOAD_DIR, f"{id_str}{ext}")
            with open(file_path, "wb") as f:
                f.write(file_content)
            logger.debug(f"File saved at {file_path}")

        db[id_str] = {
            "id": id_str,
            "text": text,
            "frequency": frequency,
            "file_path": file_path,
            "file_type": file_type,
            "scanner_device_id": scanner_device_id,
            "consumed": False,
            "created_at": time.time()
        }

        await send_targeted_fcm_notification(scanner_device_id, id_str, text, file_type)
        
        logger.info(f"Upload successful for id={id_str}")
    return id_str

async def send_targeted_fcm_notification(device_id: str, data_id: str, text: str, file_type: Optional[str]):
    if device_id not in device_registrations:
        logger.warning(f"No FCM token registered for device {device_id}")
        return

    token = device_registrations[device_id]
    
    message = messaging.Message(
        token=token,
        data={
            "id": data_id,
            "text": text,
            "fileType": file_type or "Nessun file"
        },
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(
                title="Nuovi dati disponibili",
                body=f"ID: {data_id} - {text[:30]}...",
                priority="high",
                visibility="public",
                channel_id="HIGH_PRIORITY_CHANNEL"
            )
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    content_available=True,
                    alert=messaging.ApsAlert(
                        title="Nuovi dati disponibili",
                        body=f"ID: {data_id} - {text[:30]}..."
                    ),
                    sound="default"
                )
            )
        )
    )

    try:
        response = messaging.send(message)
        logger.info(f"Sent targeted FCM notification to device {device_id}")
    except Exception as e:
        logger.error(f"Error sending FCM to device {device_id}: {e}")

async def upload_worker():
    logger.info("Upload worker started")
    while True:
        job = await upload_queue.get()
        logger.debug(f"Got upload job from queue: {job}")
        try:
            await process_upload_job(job)
        except Exception as e:
            logger.error(f"Error processing upload job {job}: {e}", exc_info=True)
        finally:
            upload_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(upload_worker())
    cleanup_task = asyncio.create_task(cleanup_old_entries())
    yield
    worker_task.cancel()
    cleanup_task.cancel()
    try:
        await worker_task
        await cleanup_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.post("/fcm/register")
async def register_fcm_token(request: FcmTokenRequest):
    device_registrations[request.device_id] = request.token
    logger.info(f"Registered FCM token for device {request.device_id}")
    return {"status": "success", "message": "Token registered"}

@app.post("/protocol/upload")
async def protocol_upload(
    text: str = Form(...),
    id: str = Form(...),
    frequency: Optional[int] = Form(0),
    file: Optional[UploadFile] = File(None),
    scanner_device_id: str = Form(...)
):
    logger.debug(f"Received upload request from device {scanner_device_id}")
    try:
        file_content = await file.read() if file else None
        filename = file.filename if file else ""
        content_type = file.content_type if file else ""
        await upload_queue.put((id, text, file_content, filename, content_type, frequency, scanner_device_id))
        return {"status": "queued", "id": id}
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/protocol/get")
async def protocol_get(id: str, request: Request):
    logger.info(f"GET data for id={id}")
    if id not in db:
        logger.warning(f"Data not found for id={id}")
        raise HTTPException(status_code=404, detail="Data not found")

    entry = db[id]
    entry["created_at"] = time.time()

    file_url = None
    if entry["file_path"]:
        base_url = str(request.base_url).rstrip("/")
        file_url = f"{base_url}/protocol/download?id={id}"
        logger.debug(f"Returning file URL: {file_url}")

    return {
        "id": entry["id"],
        "text": entry["text"],
        "fileUrl": file_url,
        "fileType": entry["file_type"],
        "scanner_device_id": entry["scanner_device_id"],
        "consumed": entry["consumed"],
        "created_at": entry["created_at"]
    }

@app.get("/protocol/download")
async def download_file(id: str):
    if id not in db:
        raise HTTPException(status_code=404, detail="File not found")

    entry = db[id]
    if not entry["file_path"] or not os.path.exists(entry["file_path"]):
        raise HTTPException(status_code=404, detail="File not found")

    entry["created_at"] = time.time()

    return FileResponse(
        entry["file_path"],
        media_type=entry["file_type"],
        filename=f"file_{id}{os.path.splitext(entry['file_path'])[1]}"
    )

@app.get("/image/{image_name}")
async def get_image(image_name: str):
    path = os.path.join(UPLOAD_DIR, image_name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)

@app.get("/protocol/status")
async def get_status():
    return {
        "status": "running",
        "entries_count": len(db),
        "registered_devices": len(device_registrations),
        "upload_queue_size": upload_queue.qsize(),
        "timestamp": time.time()
    }
