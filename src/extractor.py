import sys
from typing import Callable

from ollama import chat

from .config import OCR_MODEL, EXTRACTION_MODEL
from .pdf_utils import pdf_to_images_base64
from .prompt import build_ocr_prompt, build_extraction_system_prompt, build_extraction_user_prompt
from .json_utils import extract_json

_default_log: Callable[[str], None] = lambda msg: print(msg, file=sys.stderr)


def _ocr_page(image_b64: str, page_num: int, prefix: str, log: Callable[[str], None]) -> str:
    log(f"{prefix}  OCR страницы {page_num} ({OCR_MODEL})...")
    response = chat(
        model=OCR_MODEL,
        messages=[
            {
                "role": "user",
                "content": build_ocr_prompt(),
                "images": [image_b64],
            }
        ],
        options={"temperature": 0},
    )
    return response.message.content or ""


def extract_fields(pdf_path: str, log: Callable[[str], None] = _default_log) -> dict:
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {OCR_MODEL}...")
    page_texts = []
    for i, image in enumerate(images, start=1):
        text = _ocr_page(image, i, prefix, log)
        page_texts.append(f"=== Страница {i} ===\n{text}")

    combined_ocr = "\n\n".join(page_texts)
    log(f"{prefix}--- Результат OCR ---\n{combined_ocr}\n{prefix}--- Конец OCR ---")

    log(f"{prefix}Этап 2: извлечение полей моделью {EXTRACTION_MODEL}...")
    response = chat(
        model=EXTRACTION_MODEL,
        messages=[
            {
                "role": "system",
                "content": build_extraction_system_prompt(),
            },
            {
                "role": "user",
                "content": build_extraction_user_prompt(combined_ocr),
            },
        ],
        options={"temperature": 0},
    )

    text = response.message.content or ""
    if not text.strip() and response.message.thinking:
        log(f"{prefix}content пустой, используем thinking как fallback")
        text = response.message.thinking or ""
    log(f"{prefix}--- Сырой ответ модели ---\n{text}\n{prefix}--- Конец ответа ---")
    return extract_json(text)
