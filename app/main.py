from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.routers import presets, process, export
from app.config import PRESETS_DIR, OUTPUT_DIR

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    PRESETS_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    yield


app = FastAPI(title="Document Recognizer", lifespan=lifespan)

app.include_router(presets.router, prefix="/api")
app.include_router(process.router, prefix="/api")
app.include_router(export.router, prefix="/api")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(STATIC_DIR / "index.html")
