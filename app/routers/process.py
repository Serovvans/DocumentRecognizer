import asyncio
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from app.config import OUTPUT_DIR
from app.services.scanner import scan_folder
from app.services.processor import process_documents
from app.services.db_writer import DBWriter

router = APIRouter()

# In-memory session store: session_id → summary dict
_sessions: dict[str, dict] = {}


class ScanRequest(BaseModel):
    folder: str


@router.post("/scan")
async def scan(req: ScanRequest):
    try:
        files = scan_folder(req.folder)
    except FileNotFoundError as e:
        raise HTTPException(400, str(e))
    return {"files": files, "count": len(files)}


@router.get("/session/{session_id}")
async def get_session(session_id: str):
    if session_id not in _sessions:
        raise HTTPException(404, "Session not found")
    return _sessions[session_id]


@router.websocket("/ws/process")
async def process_ws(websocket: WebSocket):
    await websocket.accept()

    try:
        config = await asyncio.wait_for(websocket.receive_json(), timeout=30)
    except (asyncio.TimeoutError, Exception):
        await websocket.close()
        return

    session_id = str(uuid.uuid4())
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"{session_id}.jsonl"

    loop = asyncio.get_event_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def callback(update: dict) -> None:
        asyncio.run_coroutine_threadsafe(queue.put(update), loop)

    # Optionally set up DB writer
    db_writer: DBWriter | None = None
    if config.get("db_enabled"):
        db_writer = DBWriter(
            schema=config.get("db_schema", "public"),
            table=config.get("db_table", "documents"),
            fields=[f["name"] for f in config.get("fields", [])],
        )
        try:
            db_writer.start(max_workers=config.get("workers", 2))
        except Exception as e:
            await websocket.send_json({"type": "error", "message": f"DB connection failed: {e}"})
            await websocket.close()
            return

    # Start processing in a thread (blocking Ollama calls)
    processing_future = loop.run_in_executor(
        None,
        lambda: process_documents(
            pdf_paths=config.get("files", []),
            fields=config.get("fields", []),
            workers=config.get("workers", 2),
            callback=callback,
            output_path=str(output_path),
            db_writer=db_writer,
        ),
    )

    # Stream progress events to the client
    try:
        while True:
            try:
                update = await asyncio.wait_for(queue.get(), timeout=60.0)
                update["session_id"] = session_id
                await websocket.send_json(update)
                if update.get("type") in ("complete", "error"):
                    _sessions[session_id] = update
                    break
            except asyncio.TimeoutError:
                # Keepalive ping so the browser doesn't close the socket
                try:
                    await websocket.send_json({"type": "keepalive"})
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    finally:
        if db_writer:
            db_writer.stop()

    # Ensure the background thread has finished
    try:
        await processing_future
    except Exception:
        pass
