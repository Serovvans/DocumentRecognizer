import sys

from ollama import chat

from .config import OCR_MODEL, EXTRACTION_MODEL
from .pdf_utils import pdf_to_images_base64
from .prompt import build_ocr_prompt, build_extraction_system_prompt, build_extraction_user_prompt
from .json_utils import extract_json


def _ocr_page(image_b64: str, page_num: int) -> str:
    print(f"  OCR страницы {page_num} ({OCR_MODEL})...", file=sys.stderr)
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


def extract_fields(pdf_path: str) -> dict:
    print(f"Конвертация PDF в изображения: {pdf_path}", file=sys.stderr)
    images = pdf_to_images_base64(pdf_path)

    # Этап 1: OCR каждой страницы через glm-ocr
    print(f"Этап 1: распознавание {len(images)} страниц моделью {OCR_MODEL}...", file=sys.stderr)
    page_texts = []
    for i, image in enumerate(images, start=1):
        text = _ocr_page(image, i)
        page_texts.append(f"=== Страница {i} ===\n{text}")

    combined_ocr = "\n\n".join(page_texts)
    print("--- Результат OCR ---", file=sys.stderr)
    print(combined_ocr, file=sys.stderr)
    print("--- Конец OCR ---", file=sys.stderr)

    # Этап 2: извлечение полей через llama3.1:8b
    print(f"Этап 2: извлечение полей моделью {EXTRACTION_MODEL}...", file=sys.stderr)
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

    print("Ответ получен.", file=sys.stderr)
    text = response.message.content or ""
    if not text.strip() and response.message.thinking:
        print("content пустой, используем thinking как fallback", file=sys.stderr)
        text = response.message.thinking or ""
    print("--- Сырой ответ модели ---", file=sys.stderr)
    print(text, file=sys.stderr)
    print("--- Конец ответа ---", file=sys.stderr)
    return extract_json(text)
