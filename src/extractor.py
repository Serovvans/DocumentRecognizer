import sys
from typing import Callable

from ollama import chat

from .config import OCR_MODEL, EXTRACTION_MODEL
from .pdf_utils import pdf_to_images_base64
from .prompt import (
    build_ocr_prompt,
    build_extraction_system_prompt,
    build_extraction_system_prompt_dynamic,
    build_extraction_user_prompt,
    build_classification_system_prompt,
    build_classification_user_prompt,
)
from .json_utils import extract_json


class DocumentRejected(Exception):
    """Raised when a document does not match the user-supplied classification filter."""
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

_default_log: Callable[[str], None] = lambda msg: print(msg, file=sys.stderr)


def _ocr_page(
    image_b64: str,
    page_num: int,
    prefix: str,
    log: Callable[[str], None],
    ocr_model: str = OCR_MODEL,
) -> str:
    log(f"{prefix}  OCR страницы {page_num} ({ocr_model})...")
    response = chat(
        model=ocr_model,
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


def _classify_document(
    ocr_text: str,
    classification_prompt: str,
    prefix: str,
    log: Callable[[str], None],
    model: str = EXTRACTION_MODEL,
) -> None:
    """Check document relevance; raises DocumentRejected if the document does not match."""
    log(f"{prefix}Классификация документа моделью {model}...")
    response = chat(
        model=model,
        messages=[
            {"role": "system", "content": build_classification_system_prompt()},
            {"role": "user", "content": build_classification_user_prompt(classification_prompt, ocr_text)},
        ],
        options={"temperature": 0},
    )
    text = response.message.content or ""
    try:
        data = extract_json(text)
        relevant = bool(data.get("relevant", True))
        reason = str(data.get("reason", ""))
    except (ValueError, AttributeError):
        # If we can't parse the answer, default to keeping the document
        log(f"{prefix}Не удалось разобрать ответ классификатора, документ сохранён")
        return
    if not relevant:
        log(f"{prefix}Документ отклонён классификатором: {reason}")
        raise DocumentRejected(reason)
    log(f"{prefix}Документ принят классификатором: {reason}")


def extract_fields_dynamic(
    pdf_path: str,
    fields: list[dict],
    log: Callable[[str], None] = _default_log,
    ocr_model: str = OCR_MODEL,
    extraction_model: str = EXTRACTION_MODEL,
    classification_prompt: str = "",
) -> dict:
    """Like extract_fields but uses caller-supplied field definitions and models.

    If *classification_prompt* is provided, a classification step runs after OCR.
    Raises DocumentRejected if the document does not match the prompt.
    """
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {ocr_model}...")
    page_texts = []
    for i, image in enumerate(images, start=1):
        text = _ocr_page(image, i, prefix, log, ocr_model=ocr_model)
        page_texts.append(f"=== Страница {i} ===\n{text}")

    combined_ocr = "\n\n".join(page_texts)

    if classification_prompt.strip():
        _classify_document(combined_ocr, classification_prompt, prefix, log, model=extraction_model)

    log(f"{prefix}Этап 2: извлечение полей моделью {extraction_model}...")
    response = chat(
        model=extraction_model,
        messages=[
            {"role": "system", "content": build_extraction_system_prompt_dynamic(fields)},
            {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
        ],
        options={"temperature": 0},
    )

    text = response.message.content or ""
    if not text.strip() and response.message.thinking:
        log(f"{prefix}content пустой, используем thinking как fallback")
        text = response.message.thinking or ""
    log(f"{prefix}--- Сырой ответ модели ---\n{text}\n{prefix}--- Конец ответа ---")
    return extract_json(text)
