#!/usr/bin/env python3
"""
Extractor of fields from scanned construction permit PDFs.
Stage 1: glm-ocr recognizes text and tables from each page.
Stage 2: llama3.1:8b extracts structured fields from OCR text into JSON.
"""

import sys
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.extractor import extract_fields
from src.config import MAX_WORKERS

_stderr_lock = threading.Lock()


def _log(msg: str) -> None:
    with _stderr_lock:
        print(msg, file=sys.stderr)


def _process_one(pdf_path: str) -> tuple[str, dict | str]:
    try:
        result = extract_fields(pdf_path, log=_log)
        return pdf_path, result
    except ConnectionError:
        return pdf_path, "Ошибка: не удалось подключиться к ollama"
    except Exception as e:
        return pdf_path, f"Ошибка: {e}"


def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py <pdf1> [pdf2 ...]", file=sys.stderr)
        sys.exit(1)

    pdf_paths = sys.argv[1:]
    workers = min(MAX_WORKERS, len(pdf_paths))

    _log(f"Обработка {len(pdf_paths)} документов (потоков: {workers})...")

    results: dict[str, object] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one, p): p for p in pdf_paths}
        for future in as_completed(futures):
            path, result = future.result()
            results[path] = result
            _log(f"Готово: {path}")

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
