import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from ollama import chat

from .config import OCR_MODEL, EXTRACTION_MODEL, OCR_PAGE_WORKERS
from .pdf_utils import pdf_to_images_base64
from .prompt import (
    build_ocr_prompt,
    build_extraction_system_prompt,
    build_extraction_system_prompt_dynamic,
    build_extraction_user_prompt,
    build_classification_system_prompt,
    build_classification_user_prompt,
    build_json_fix_prompt,
)
from .json_utils import extract_json, get_last_parse_error

_MAX_JSON_RETRIES = 3


class DocumentRejected(Exception):
    """Raised when a document does not match the user-supplied classification filter."""
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

_default_log: Callable[[str], None] = lambda msg: print(msg, file=sys.stderr)


def _extract_with_retry(
    messages: list,
    model: str,
    prefix: str,
    log: Callable[[str], None],
) -> dict:
    for attempt in range(_MAX_JSON_RETRIES):
        response = chat(model=model, messages=messages, options=_EXTRACT_OPTIONS)
        text = response.message.content or ""
        if not text.strip() and response.message.thinking:
            log(f"{prefix}content пустой, используем thinking как fallback")
            text = response.message.thinking or ""
        log(f"{prefix}--- Сырой ответ модели (попытка {attempt + 1}) ---\n{text}\n{prefix}--- Конец ответа ---")
        try:
            return extract_json(text)
        except ValueError as e:
            if attempt == _MAX_JSON_RETRIES - 1:
                raise
            parse_error = get_last_parse_error(text)
            log(f"{prefix}Попытка {attempt + 1}: ошибка парсинга JSON ({parse_error}), запрашиваем исправление у модели...")
            messages.append({"role": "assistant", "content": text})
            messages.append({"role": "user", "content": build_json_fix_prompt(parse_error)})
    raise RuntimeError("unreachable")


_OCR_OPTIONS = {
    "temperature": 0,
    "num_batch": 2048,
    "num_predict": 3072,
}

_EXTRACT_OPTIONS = {
    "temperature": 0,
    "num_batch": 2048,
    "num_predict": 2048,
}


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
        options=_OCR_OPTIONS,
    )
    return response.message.content or ""


def _ocr_pages_parallel(
    images: list[str],
    prefix: str,
    log: Callable[[str], None],
    ocr_model: str = OCR_MODEL,
    max_workers: int = 4,
) -> list[str]:
    """OCR all pages concurrently; returns texts in page order."""
    results: dict[int, str] = {}

    def _task(args: tuple[int, str]) -> tuple[int, str]:
        page_num, image_b64 = args
        return page_num, _ocr_page(image_b64, page_num, prefix, log, ocr_model)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_task, (i, img)): i for i, img in enumerate(images, start=1)}
        for future in as_completed(futures):
            page_num, text = future.result()
            results[page_num] = text

    return [f"=== Страница {i} ===\n{results[i]}" for i in sorted(results)]


def extract_fields(pdf_path: str, log: Callable[[str], None] = _default_log) -> dict:
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {OCR_MODEL} (параллельно)...")
    page_texts = _ocr_pages_parallel(images, prefix, log, max_workers=OCR_PAGE_WORKERS)

    combined_ocr = "\n\n".join(page_texts)
    log(f"{prefix}--- Результат OCR ---\n{combined_ocr}\n{prefix}--- Конец OCR ---")

    log(f"{prefix}Этап 2: извлечение полей моделью {EXTRACTION_MODEL}...")
    messages = [
        {"role": "system", "content": build_extraction_system_prompt()},
        {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
    ]
    return _extract_with_retry(messages, EXTRACTION_MODEL, prefix, log)


def _classify_document(
    ocr_text: str,
    classification_prompt: str,
    prefix: str,
    log: Callable[[str], None],
    model: str = EXTRACTION_MODEL,
    fields: list[dict] | None = None,
) -> None:
    """Check document relevance; raises DocumentRejected if the document does not match."""
    log(f"{prefix}Классификация документа моделью {model}...")
    response = chat(
        model=model,
        messages=[
            {"role": "system", "content": build_classification_system_prompt()},
            {"role": "user", "content": build_classification_user_prompt(classification_prompt, ocr_text, fields)},
        ],
        options={**_EXTRACT_OPTIONS, "num_predict": 256},
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

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {ocr_model} (параллельно)...")
    page_texts = _ocr_pages_parallel(images, prefix, log, ocr_model=ocr_model, max_workers=OCR_PAGE_WORKERS)

    combined_ocr = "\n\n".join(page_texts)

    if classification_prompt.strip():
        _classify_document(combined_ocr, classification_prompt, prefix, log, model=extraction_model, fields=fields)

    log(f"{prefix}Этап 2: извлечение полей моделью {extraction_model}...")
    messages = [
        {"role": "system", "content": build_extraction_system_prompt_dynamic(fields)},
        {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
    ]
    return _extract_with_retry(messages, extraction_model, prefix, log)
