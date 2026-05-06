import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from ollama import chat

from .config import OCR_MODEL, EFFECTIVE_EXTRACTION_MODEL, OCR_PAGE_WORKERS
from .html_utils import html_tables_to_text
from .llm import call_text_model
from .pdf_utils import pdf_to_images_base64
from .prompt import (
    build_ocr_prompt,
    build_extraction_system_prompt,
    build_extraction_system_prompt_dynamic,
    build_extraction_user_prompt,
    build_section_extraction_system_prompt,
    build_single_field_system_prompt,
    build_single_field_user_prompt,
    build_classification_system_prompt,
    build_classification_user_prompt,
    build_json_fix_prompt,
)
from .json_utils import extract_json, get_last_parse_error

_MAX_JSON_RETRIES = 3

_VALID_RU_MONTHS = frozenset({
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
})

_NULL_LIKE_VALUES = frozenset({
    "нет", "не указано", "не найдено", "отсутствует", "отсутствует в документе",
    "нет данных", "не определено", "не установлено", "не указан", "не указана",
    "н/д", "нд", "-", "—", "",
})


def _postprocess(result: dict, fields: list) -> dict:
    """Ensure all fields present; replace null-like strings with None; unwrap lists.

    *fields* may be a list of str (legacy) or a list of dicts with 'name' and
    optional 'allow_list' (bool, default False).  When allow_list is False and
    the model returned a list, only the first non-null element is kept — this
    prevents hallucinated multi-values for inherently single-value fields.
    """
    out: dict = {}
    for f in fields:
        if isinstance(f, str):
            name = f
            allow_list = True  # legacy path: preserve existing behaviour
        else:
            name = f["name"]
            allow_list = bool(f.get("allow_list", False))

        val = result.get(name)
        if isinstance(val, list):
            cleaned = [v for v in val if not (isinstance(v, str) and v.strip().lower() in _NULL_LIKE_VALUES)]
            if not cleaned:
                out[name] = None
            elif not allow_list:
                out[name] = cleaned[0]
            elif len(cleaned) == 1:
                out[name] = cleaned[0]
            else:
                out[name] = cleaned
        elif isinstance(val, str) and val.strip().lower() in _NULL_LIKE_VALUES:
            out[name] = None
        else:
            out[name] = val
    return out


def _has_handwriting_issues(data: dict, fields: list) -> bool:
    """Return True if extracted date fields show signs of handwriting OCR errors.

    Checks for: garbled month names (e.g. "месл" instead of "июля") and
    year digits split by spaces (e.g. "20 19" instead of "2019").
    Only applied to fields with db_type == "date" to avoid false positives.
    """
    for field in fields:
        if isinstance(field, str) or field.get("db_type") != "date":
            continue
        name = field["name"]
        val = data.get(name)
        if val is None:
            continue
        vals = val if isinstance(val, list) else [val]
        for v in vals:
            if not isinstance(v, str):
                continue
            if re.search(r'\d\s+\d', v):
                return True
            for word in re.findall(r'[а-яёА-ЯЁ]{3,}', v.lower()):
                if word not in _VALID_RU_MONTHS:
                    return True
    return False


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
    raw_collector: dict | None = None,
) -> dict:
    for attempt in range(_MAX_JSON_RETRIES):
        text = call_text_model(messages, model, log=log)
        log(f"{prefix}--- Сырой ответ модели (попытка {attempt + 1}) ---\n{text}\n{prefix}--- Конец ответа ---")
        try:
            result = extract_json(text)
            if raw_collector is not None:
                raw_collector[prefix] = text
            return result
        except ValueError:
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
    "num_predict": 8192,  # было 3072
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

    log(f"{prefix}Этап 2: извлечение полей моделью {EFFECTIVE_EXTRACTION_MODEL}...")
    messages = [
        {"role": "system", "content": build_extraction_system_prompt()},
        {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
    ]
    raw = _extract_with_retry(messages, EFFECTIVE_EXTRACTION_MODEL, prefix, log)
    return _postprocess(raw, FIELDS)


_FIELD_WORKERS = 8


def _extract_single_field(
    field_name: str,
    field_description: str,
    ocr_text: str,
    model: str,
    prefix: str,
    log: Callable[[str], None],
) -> object:
    """Extract one field from OCR text; returns the field value (str, list, or None)."""
    messages = [
        {"role": "system", "content": build_single_field_system_prompt(field_name, field_description)},
        {"role": "user", "content": build_single_field_user_prompt(field_name, ocr_text)},
    ]
    result = _extract_with_retry(messages, model, prefix, log)
    return result.get(field_name)


def _extract_fields_per_field(
    ocr_text: str,
    fields: list[dict],
    model: str,
    prefix: str,
    log: Callable[[str], None],
) -> dict:
    """Extract each field in a separate LLM call, parallelised."""
    result: dict[str, object] = {}
    lock = threading.Lock()

    def _task(field: dict) -> None:
        name = field["name"]
        desc = field.get("description", "")
        log(f"{prefix}  Извлечение поля «{name}»...")
        value = _extract_single_field(name, desc, ocr_text, model, prefix, log)
        with lock:
            result[name] = value

    with ThreadPoolExecutor(max_workers=_FIELD_WORKERS) as pool:
        futures = [pool.submit(_task, f) for f in fields]
        for future in as_completed(futures):
            future.result()  # propagate exceptions

    # preserve original field order
    return {f["name"]: result[f["name"]] for f in fields}


def _extract_section(
    section_name: str,
    section_description: str,
    fields: list[dict],
    ocr_text: str,
    model: str,
    prefix: str,
    log: Callable[[str], None],
) -> dict:
    messages = [
        {"role": "system", "content": build_section_extraction_system_prompt(section_name, section_description, fields)},
        {"role": "user", "content": build_extraction_user_prompt(ocr_text)},
    ]
    return _extract_with_retry(messages, model, prefix, log)


def _extract_fields_by_sections(
    ocr_text: str,
    sections: list[dict],
    model: str,
    prefix: str,
    log: Callable[[str], None],
) -> dict:
    """Extract fields section by section, parallelised across sections."""
    result: dict[str, object] = {}
    lock = threading.Lock()

    def _task(section: dict) -> None:
        fields = section.get("fields", [])
        if not fields:
            return
        name = section["name"]
        desc = section.get("description", "")
        log(f"{prefix}  Раздел «{name}» ({len(fields)} полей)...")
        raw = _extract_section(name, desc, fields, ocr_text, model, prefix, log)
        with lock:
            result.update(raw)

    with ThreadPoolExecutor(max_workers=_FIELD_WORKERS) as pool:
        futures = [pool.submit(_task, s) for s in sections]
        for future in as_completed(futures):
            future.result()

    return result


def _classify_document(
    ocr_text: str,
    classification_prompt: str,
    prefix: str,
    log: Callable[[str], None],
    model: str = EFFECTIVE_EXTRACTION_MODEL,
    fields: list[dict] | None = None,
) -> None:
    """Check document relevance; raises DocumentRejected if the document does not match."""
    log(f"{prefix}Классификация документа моделью {model}...")
    text = call_text_model(
        messages=[
            {"role": "system", "content": build_classification_system_prompt()},
            {"role": "user", "content": build_classification_user_prompt(classification_prompt, ocr_text, fields)},
        ],
        model=model,
        max_tokens=256,
    )
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


def ocr_document(
    pdf_path: str,
    ocr_model: str = OCR_MODEL,
    log: Callable[[str], None] = _default_log,
) -> str:
    """OCR all pages of *pdf_path*; return concatenated text with page separators."""
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)
    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {ocr_model} (параллельно)...")
    page_texts = _ocr_pages_parallel(images, prefix, log, ocr_model=ocr_model, max_workers=OCR_PAGE_WORKERS)
    combined_ocr = "\n\n".join(page_texts)
    log(f"{prefix}--- Результат OCR ---\n{combined_ocr}\n{prefix}--- Конец OCR ---")
    return combined_ocr


def extract_fields_from_ocr(
    ocr_text: str,
    fields: list[dict],
    log: Callable[[str], None] = _default_log,
    extraction_model: str = EFFECTIVE_EXTRACTION_MODEL,
    classification_prompt: str = "",
    per_field: bool = False,
    sections: list[dict] | None = None,
    prefix: str = "",
    raw_collector: dict | None = None,
) -> dict:
    """Run LLM field extraction on already-OCR'd text.

    Mirrors the second half of extract_fields_dynamic, decoupled from the OCR step.
    *fields* must be a list of dicts with at least a 'name' key.
    If *raw_collector* is provided (a dict), raw model responses are stored there
    keyed by *prefix* for diagnostic purposes.
    """
    # Convert HTML table output to plain text before passing to the extraction LLM.
    # The original ocr_text (with HTML) is kept intact for OCR auxiliary file saving.
    extraction_text = html_tables_to_text(ocr_text)

    if classification_prompt.strip():
        _classify_document(extraction_text, classification_prompt, prefix, log, model=extraction_model, fields=fields)

    if sections:
        log(f"{prefix}Этап 2: извлечение по {len(sections)} разделам моделью {extraction_model}...")
        raw = _extract_fields_by_sections(extraction_text, sections, extraction_model, prefix, log)
        result = _postprocess(raw, fields)
        result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
        return result

    if per_field:
        log(f"{prefix}Этап 2: извлечение полей по одному (per-field) моделью {extraction_model}...")
        raw = _extract_fields_per_field(extraction_text, fields, extraction_model, prefix, log)
        result = _postprocess(raw, fields)
        result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
        return result

    log(f"{prefix}Этап 2: извлечение полей моделью {extraction_model}...")
    messages = [
        {"role": "system", "content": build_extraction_system_prompt_dynamic(fields)},
        {"role": "user", "content": build_extraction_user_prompt(extraction_text)},
    ]
    raw = _extract_with_retry(messages, extraction_model, prefix, log, raw_collector=raw_collector)
    result = _postprocess(raw, fields)
    result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
    return result


def extract_fields_dynamic(
    pdf_path: str,
    fields: list[dict],
    log: Callable[[str], None] = _default_log,
    ocr_model: str = OCR_MODEL,
    extraction_model: str = EFFECTIVE_EXTRACTION_MODEL,
    classification_prompt: str = "",
    per_field: bool = False,
    sections: list[dict] | None = None,
) -> dict:
    """Like extract_fields but uses caller-supplied field definitions and models.

    If *classification_prompt* is provided, a classification step runs after OCR.
    Raises DocumentRejected if the document does not match the prompt.

    If *per_field* is True, each field is extracted in a separate LLM call
    (parallelised). This reduces the chance of fields being silently skipped
    when the model has to handle many fields at once.
    """
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {ocr_model} (параллельно)...")
    page_texts = _ocr_pages_parallel(images, prefix, log, ocr_model=ocr_model, max_workers=OCR_PAGE_WORKERS)

    combined_ocr = "\n\n".join(page_texts)

    if classification_prompt.strip():
        _classify_document(combined_ocr, classification_prompt, prefix, log, model=extraction_model, fields=fields)

    if sections:
        log(f"{prefix}Этап 2: извлечение по {len(sections)} разделам моделью {extraction_model}...")
        raw = _extract_fields_by_sections(combined_ocr, sections, extraction_model, prefix, log)
        result = _postprocess(raw, fields)
        result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
        return result

    field_names = [f["name"] for f in fields]

    if per_field:
        log(f"{prefix}Этап 2: извлечение полей по одному (per-field) моделью {extraction_model}...")
        raw = _extract_fields_per_field(combined_ocr, fields, extraction_model, prefix, log)
        result = _postprocess(raw, fields)
        result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
        return result

    log(f"{prefix}Этап 2: извлечение полей моделью {extraction_model}...")
    messages = [
        {"role": "system", "content": build_extraction_system_prompt_dynamic(fields)},
        {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
    ]
    raw = _extract_with_retry(messages, extraction_model, prefix, log)
    result = _postprocess(raw, fields)
    result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
    return result
