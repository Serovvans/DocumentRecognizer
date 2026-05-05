import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from ollama import chat

from .config import OCR_MODEL, EFFECTIVE_EXTRACTION_MODEL, OCR_PAGE_WORKERS
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
from .html_parser import parse_ocr_html, is_modern_format

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

# ИНН: 10 цифр (юрлицо) или 12 (физлицо/ИП)
_INN_LENGTHS = frozenset({10, 12})
# ОГРН: 13 цифр (юрлицо) или 15 (ИП — ОГРНИП)
_OGRN_LENGTHS = frozenset({13, 15})


def _digits_only(s: str) -> str:
    return re.sub(r'\D', '', s)


def _is_valid_inn(s: str) -> bool:
    return len(_digits_only(s)) in _INN_LENGTHS


def _is_valid_ogrn(s: str) -> bool:
    return len(_digits_only(s)) in _OGRN_LENGTHS


def _clean_numeric_id(val) -> str | None:
    """Strip non-digit characters; return cleaned string or None if result is empty."""
    if not isinstance(val, str):
        return None
    cleaned = _digits_only(val)
    return cleaned if cleaned else None


def _postprocess(result: dict, fields: list) -> dict:
    """Ensure all fields present; replace null-like strings with None; unwrap lists.

    *fields* may be a list of str (legacy) or a list of dicts with 'name' and
    optional 'allow_list' (bool, default False).  When allow_list is False and
    the model returned a list, only the first non-null element is kept — this
    prevents hallucinated multi-values for inherently single-value fields.

    For fields with db_type 'inn' or 'ogrn' in the dynamic preset, values are
    validated: non-digit characters are stripped, and values with the wrong digit
    count are set to null.  If inn and ogrn appear swapped (digit lengths match
    the opposite field), they are automatically exchanged.
    """
    # Build db_type map from dynamic field definitions (not available in legacy str path)
    db_type_map: dict[str, str] = {}
    for f in fields:
        if isinstance(f, dict) and "db_type" in f:
            db_type_map[f["name"]] = f["db_type"]

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

    # --- INN / OGRN format validation + auto-swap ---
    # Collect all field names that carry inn or ogrn semantics
    inn_fields = [n for n, t in db_type_map.items() if t == "inn"]
    ogrn_fields = [n for n, t in db_type_map.items() if t == "ogrn"]

    if inn_fields and ogrn_fields:
        # For now handle the common single-pair case; multi-pair is unusual
        inn_name = inn_fields[0]
        ogrn_name = ogrn_fields[0]
        inn_val = out.get(inn_name)
        ogrn_val = out.get(ogrn_name)

        inn_digits = _clean_numeric_id(inn_val) if isinstance(inn_val, str) else None
        ogrn_digits = _clean_numeric_id(ogrn_val) if isinstance(ogrn_val, str) else None

        inn_ok = inn_digits is not None and len(inn_digits) in _INN_LENGTHS
        inn_looks_ogrn = inn_digits is not None and len(inn_digits) in _OGRN_LENGTHS
        ogrn_ok = ogrn_digits is not None and len(ogrn_digits) in _OGRN_LENGTHS
        ogrn_looks_inn = ogrn_digits is not None and len(ogrn_digits) in _INN_LENGTHS

        if inn_looks_ogrn and ogrn_looks_inn:
            # Values are clearly swapped — exchange them
            out[inn_name] = ogrn_digits
            out[ogrn_name] = inn_digits
        else:
            # INN: must be 10 or 12 digits; anything else → null
            if inn_val is not None:
                if inn_ok:
                    out[inn_name] = inn_digits
                else:
                    out[inn_name] = None
            # OGRN: must be 13 or 15 digits; anything else → null
            if ogrn_val is not None:
                if ogrn_ok:
                    out[ogrn_name] = ogrn_digits
                else:
                    out[ogrn_name] = None
    else:
        # Legacy path without db_type info: at least strip non-digits for known field names
        for name in ("ИНН", "inn"):
            if name in out and isinstance(out[name], str):
                d = _clean_numeric_id(out[name])
                if d and len(d) in _INN_LENGTHS:
                    out[name] = d
                elif d is None or len(d) not in _INN_LENGTHS:
                    out[name] = None
        for name in ("ОГРН", "ogrn"):
            if name in out and isinstance(out[name], str):
                d = _clean_numeric_id(out[name])
                if d and len(d) in _OGRN_LENGTHS:
                    out[name] = d
                elif d is None or len(d) not in _OGRN_LENGTHS:
                    out[name] = None

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
) -> dict:
    for attempt in range(_MAX_JSON_RETRIES):
        text = call_text_model(messages, model, log=log)
        log(f"{prefix}--- Сырой ответ модели (попытка {attempt + 1}) ---\n{text}\n{prefix}--- Конец ответа ---")
        try:
            return extract_json(text)
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
    "num_predict": 3072,
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


def _section_present_in_ocr(section: dict, ocr_text: str) -> bool:
    """Heuristic: check if a section's content is actually present in the OCR text.

    Uses the first field's expected label prefix from the section description, or
    falls back to checking for any of the field names.  Returns True when uncertain.
    """
    desc = section.get("description", "")
    # Look for explicit numbered-section markers mentioned in the description,
    # e.g. "Раздел 2", "РАЗДЕЛ 2", "п. 2.1", "2.2.1"
    section_numbers = re.findall(r'\b(\d+\.\d+(?:\.\d+)?)', desc)
    for num in section_numbers:
        # Accept both "2.1." and "2.1 " forms in OCR text
        if re.search(re.escape(num) + r'[.\s]', ocr_text):
            return True
    # Also look for "Раздел N" keywords
    razdel_numbers = re.findall(r'Раздел\s+(\d+)', desc, re.IGNORECASE)
    for n in razdel_numbers:
        if re.search(rf'[Рр]аздел\s+{n}[^0-9]', ocr_text):
            return True
    # If no markers found in description, assume present to avoid false negatives
    return not (section_numbers or razdel_numbers)


def _extract_fields_by_sections(
    ocr_text: str,
    sections: list[dict],
    model: str,
    prefix: str,
    log: Callable[[str], None],
    modern: bool = False,
) -> dict:
    """Extract fields section by section, parallelised across sections.

    When *modern* is True and a section's content is not found in the OCR text,
    all fields for that section are set to null without calling the LLM — this
    prevents the model from picking up data from the wrong section.
    """
    result: dict[str, object] = {}
    lock = threading.Lock()

    def _task(section: dict) -> None:
        fields = section.get("fields", [])
        if not fields:
            return
        name = section["name"]
        desc = section.get("description", "")

        if modern and not _section_present_in_ocr(section, ocr_text):
            log(f"{prefix}  Раздел «{name}» отсутствует в OCR — поля = null")
            with lock:
                for f in fields:
                    result[f["name"]] = None
            return

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


_GARBLED_PATTERNS = re.compile(
    r'иисропешиим|кажите-кше|стронтельство|стропельство|касищек|касшейск|ратегая|фагегаан',
    re.IGNORECASE,
)


def _ocr_quality_score(ocr_text: str) -> float:
    """Return 0.0–1.0 quality estimate for OCR text (1.0 = clean).

    Combines two signals:
    - Ratio of known-garbled word patterns (low quality → low score)
    - Ratio of cyrillic words to all word-like tokens (high ratio → high score)
    """
    if not ocr_text.strip():
        return 0.0

    garbled_hits = len(_GARBLED_PATTERNS.findall(ocr_text))
    words = re.findall(r'[а-яёА-ЯЁa-zA-Z]{3,}', ocr_text)
    if not words:
        return 0.0

    cyrillic_words = sum(1 for w in words if re.search(r'[а-яёА-ЯЁ]', w))
    cyrillic_ratio = cyrillic_words / len(words)
    garbled_penalty = min(1.0, garbled_hits * 0.15)

    return max(0.0, cyrillic_ratio - garbled_penalty)


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

    Extraction strategy (in order of reliability):
    1. HTML-first: parse glm-ocr HTML tables directly by numbered section labels
       (works for modern post-2017 forms). Fields found this way bypass the LLM.
    2. LLM extraction: only for fields not found by HTML parser, or for old-format
       documents. Uses sections / per_field / bulk mode as configured.

    Metadata added to every result:
    - has_handwriting_issues: bool — detected OCR date garbling
    - low_ocr_quality: bool — OCR quality score below threshold
    """
    prefix = f"[{pdf_path}] "
    log(f"{prefix}Конвертация PDF в изображения...")
    images = pdf_to_images_base64(pdf_path)

    log(f"{prefix}Этап 1: распознавание {len(images)} страниц моделью {ocr_model} (параллельно)...")
    page_texts = _ocr_pages_parallel(images, prefix, log, ocr_model=ocr_model, max_workers=OCR_PAGE_WORKERS)

    combined_ocr = "\n\n".join(page_texts)

    if classification_prompt.strip():
        _classify_document(combined_ocr, classification_prompt, prefix, log, model=extraction_model, fields=fields)

    # --- HTML-first extraction ---
    modern = is_modern_format(combined_ocr)
    html_raw: dict[str, object] = {}
    if modern:
        html_raw = parse_ocr_html(combined_ocr)
        found = [k for k, v in html_raw.items() if v not in (None, "", [])]
        log(f"{prefix}HTML-парсер: найдено {len(found)} полей из структурированных таблиц")
    else:
        log(f"{prefix}Документ старого формата — HTML-парсер пропускается")

    all_field_names = {f["name"] for f in fields}
    html_found = {k for k, v in html_raw.items() if v not in (None, "", []) and k in all_field_names}

    # --- LLM extraction for remaining fields ---
    missing_fields = [f for f in fields if f["name"] not in html_found]

    raw: dict[str, object] = {}

    if missing_fields:
        if sections:
            # Only call LLM for sections that still have missing fields
            missing_names = {f["name"] for f in missing_fields}
            sections_needed = [
                {**s, "fields": [f for f in s.get("fields", []) if f["name"] in missing_names]}
                for s in sections
            ]
            sections_needed = [s for s in sections_needed if s["fields"]]

            if sections_needed:
                log(f"{prefix}Этап 2 (LLM): {len(sections_needed)} разделов, {len(missing_fields)} полей без HTML-значений...")
                raw = _extract_fields_by_sections(combined_ocr, sections_needed, extraction_model, prefix, log, modern=modern)
            else:
                log(f"{prefix}Этап 2: все поля найдены HTML-парсером, LLM не вызывается")

        elif per_field:
            log(f"{prefix}Этап 2 (LLM per-field): {len(missing_fields)} полей без HTML-значений...")
            raw = _extract_fields_per_field(combined_ocr, missing_fields, extraction_model, prefix, log)

        else:
            log(f"{prefix}Этап 2 (LLM bulk): {len(missing_fields)} полей без HTML-значений...")
            messages = [
                {"role": "system", "content": build_extraction_system_prompt_dynamic(missing_fields)},
                {"role": "user", "content": build_extraction_user_prompt(combined_ocr)},
            ]
            raw = _extract_with_retry(messages, extraction_model, prefix, log)
    else:
        log(f"{prefix}Этап 2: все поля найдены HTML-парсером, LLM не вызывается")

    # Merge: HTML results win over LLM results for fields present in both
    merged = {**raw, **html_raw}

    result = _postprocess(merged, fields)

    # Metadata
    quality = _ocr_quality_score(combined_ocr)
    result["has_handwriting_issues"] = _has_handwriting_issues(result, fields)
    result["low_ocr_quality"] = quality < 0.5

    return result
