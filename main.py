#!/usr/bin/env python3
"""
Extractor of fields from scanned construction permit PDFs.
Phase 1 (parallel): glm-ocr recognizes text and tables from each page.
Phase 2 (serial):   LLM extracts structured fields from OCR text into JSON.
"""

import sys
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.extractor import ocr_document, extract_fields_from_ocr
from src.config import MAX_WORKERS, EXTRACT_WORKERS, FIELDS, EFFECTIVE_EXTRACTION_MODEL

_stderr_lock = threading.Lock()


def _log(msg: str) -> None:
    with _stderr_lock:
        print(msg, file=sys.stderr)


# Convert legacy string FIELDS to dict format; allow_list preserves existing behaviour
# where multi-value fields (e.g. renewals) are returned as lists.
_FIELDS_DICTS = [{"name": f, "allow_list": True} for f in FIELDS]


def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py <pdf1> [pdf2 ...]", file=sys.stderr)
        sys.exit(1)

    pdf_paths = sys.argv[1:]
    ocr_workers = min(MAX_WORKERS, len(pdf_paths))
    extract_workers = min(EXTRACT_WORKERS, len(pdf_paths))

    # ── Phase 1: OCR all documents in parallel ────────────────────────────────
    _log(f"Обработка {len(pdf_paths)} документов...")
    _log(f"Фаза 1: OCR ({ocr_workers} потоков)...")

    ocr_results: dict[str, str | Exception] = {}
    with ThreadPoolExecutor(max_workers=ocr_workers) as executor:
        futures = {
            executor.submit(ocr_document, p, log=_log): p for p in pdf_paths
        }
        for future in as_completed(futures):
            path = futures[future]
            try:
                ocr_results[path] = future.result()
                _log(f"OCR готово: {path}")
            except Exception as exc:
                ocr_results[path] = exc
                _log(f"OCR ошибка [{path}]: {exc}")

    # ── Phase 2: Extract fields from OCR text ────────────────────────────────
    _log(f"Фаза 2: извлечение полей моделью {EFFECTIVE_EXTRACTION_MODEL} ({extract_workers} потоков)...")

    results: dict[str, object] = {}

    def _extract_one(path: str, ocr_text: str) -> tuple[str, dict | str]:
        try:
            return path, extract_fields_from_ocr(
                ocr_text,
                _FIELDS_DICTS,
                log=_log,
                extraction_model=EFFECTIVE_EXTRACTION_MODEL,
                prefix=f"[{path}] ",
            )
        except Exception as exc:
            return path, f"Ошибка: {exc}"

    with ThreadPoolExecutor(max_workers=extract_workers) as executor:
        futures_ext = {}
        for path, ocr_result in ocr_results.items():
            if isinstance(ocr_result, Exception):
                results[path] = f"Ошибка OCR: {ocr_result}"
            else:
                futures_ext[executor.submit(_extract_one, path, ocr_result)] = path

        for future in as_completed(futures_ext):
            path, result = future.result()
            results[path] = result
            _log(f"Готово: {path}")

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
