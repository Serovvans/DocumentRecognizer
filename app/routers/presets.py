import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import PRESETS_DIR

router = APIRouter()


class FieldDef(BaseModel):
    name: str
    description: str = ""
    multi_value_mode: str = "rows"
    db_type: str = "text"
    allow_list: bool = False


class PresetPayload(BaseModel):
    name: str
    fields: list[FieldDef]


@router.get("/presets")
async def list_presets():
    result = []
    for path in sorted(PRESETS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            result.append({"name": path.stem, "fields": data.get("fields", [])})
        except Exception:
            pass
    return result


@router.post("/presets")
async def save_preset(payload: PresetPayload):
    PRESETS_DIR.mkdir(exist_ok=True)
    path = PRESETS_DIR / f"{payload.name}.json"
    path.write_text(
        json.dumps(
            {"fields": [f.model_dump() for f in payload.fields]},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"ok": True}


@router.delete("/presets/{name}")
async def delete_preset(name: str):
    path = PRESETS_DIR / f"{name}.json"
    if not path.exists():
        raise HTTPException(404, "Preset not found")
    path.unlink()
    return {"ok": True}
