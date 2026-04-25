import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.config import OUTPUT_DIR

router = APIRouter()


@router.get("/download/{session_id}")
async def download_results(session_id: str):
    # Basic path safety: session IDs are UUID4 (only hex + dashes)
    if not all(c in "0123456789abcdef-" for c in session_id):
        raise HTTPException(400, "Invalid session id")

    path = OUTPUT_DIR / f"{session_id}.jsonl"
    if not path.exists():
        raise HTTPException(404, "Session not found or results not ready")

    def generate():
        yield b"["
        first = True
        with open(path, "rb") as f:
            for raw_line in f:
                line = raw_line.strip()
                if line:
                    if not first:
                        yield b","
                    yield line
                    first = False
        yield b"]"

    filename = f"results_{session_id[:8]}.json"
    return StreamingResponse(
        generate(),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/preview/{session_id}")
async def preview_results(session_id: str):
    if not all(c in "0123456789abcdef-" for c in session_id):
        raise HTTPException(400, "Invalid session id")

    path = OUTPUT_DIR / f"{session_id}.jsonl"
    if not path.exists():
        raise HTTPException(404, "Session not found or results not ready")

    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    return records
