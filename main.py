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
import json

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)

# Configurazione Firebase
try:
    firebase_key_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if firebase_key_json:
        firebase_creds = json.loads(firebase_key_json)
        cred = credentials.Certificate(firebase_creds)
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized successfully")
    else:
        logger.warning("Firebase credentials not found, notifications will be disabled")
except Exception as e:
    logger.error(f"Firebase initialization failed: {e}")

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

db = {}
device_registrations: Dict[str, str] = {}
upload_lock = asyncio.Lock()
upload_queue = asyncio.Queue()

class FcmTokenRequest(BaseModel):
    token: str
    device_id: str

async def cleanup_old_entries():
    while True:
        try:
            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)
            to_delete = []
            
            for id, entry in db.items():
                if "created_at" not in entry:
                    entry["created_at"] = time.time()
                    continue
                if datetime.fromtimestamp(entry["created_at"]) < one_hour_ago:
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
    id_str, text, file_content, filename, content_type, frequency = job
    logger.debug(f"Processing upload job: id={id_str}, frequency={frequency}")

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
            "consumed": False,
            "created_at": time.time()
        }

        # Invia notifica a tutti i dispositivi registrati
        await send_broadcast_fcm_notification(id_str, text, file_type)
        
        logger.info(f"Upload successful for id={id_str}")
    return id_str

async def send_broadcast_fcm_notification(data_id: str, text: str, file_type: Optional[str]):
    if not device_registrations:
        logger.debug("No devices registered for notifications")
        return

    for device_id, token in device_registrations.items():
        try:
            message = messaging.Message(
                token=token,
                data={
                    "id": data_id,
                    "text": text,
                    "fileType": file_type or "none"
                },
                notification=messaging.Notification(
                    title="Nuovo contenuto disponibile",
                    body=f"ID: {data_id} - {text[:50]}..."
                ),
                android=messaging.AndroidConfig(
                    priority="high"
                )
            )
            messaging.send(message)
            logger.debug(f"Notification sent to device {device_id}")
        except Exception as e:
            logger.error(f"Failed to send notification to device {device_id}: {e}")

async def upload_worker():
    logger.info("Upload worker started")
    while True:
        job = await upload_queue.get()
        logger.debug(f"Got upload job from queue: {job[0]}")
        try:
            await process_upload_job(job)
        except Exception as e:
            logger.error(f"Error processing upload job: {e}", exc_info=True)
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
    logger.info(f"Registered device {request.device_id}")
    return {"status": "success"}

@app.post("/protocol/upload")
async def protocol_upload(
    text: str = Form(...),
    id: str = Form(...),
    frequency: Optional[int] = Form(0),
    file: Optional[UploadFile] = File(None)
):
    logger.debug(f"Received upload request: id={id}, frequency={frequency}")
    try:
        file_content = await file.read() if file else None
        filename = file.filename if file else ""
        content_type = file.content_type if file else ""
        await upload_queue.put((id, text, file_content, filename, content_type, frequency))
        return {"status": "queued", "id": id}
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/protocol/get")
async def protocol_get(id: str, request: Request):
    if id not in db:
        raise HTTPException(status_code=404, detail="Data not found")

    entry = db[id]
    entry["created_at"] = time.time()

    file_url = None
    if entry["file_path"]:
        base_url = str(request.base_url).rstrip("/")
        file_url = f"{base_url}/protocol/download?id={id}"

    return {
        "id": entry["id"],
        "text": entry["text"],
        "fileUrl": file_url,
        "fileType": entry["file_type"],
        "consumed": entry["consumed"],
        "created_at": entry["created_at"]
    }

@app.get("/protocol/download")
async def download_file(id: str):
    if id not in db or not db[id]["file_path"] or not os.path.exists(db[id]["file_path"]):
        raise HTTPException(status_code=404, detail="File not found")

    db[id]["created_at"] = time.time()
    return FileResponse(
        db[id]["file_path"],
        media_type=db[id]["file_type"],
        filename=f"file_{id}{os.path.splitext(db[id]['file_path'])[1]}"
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