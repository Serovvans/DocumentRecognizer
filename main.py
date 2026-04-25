#!/usr/bin/env python3
"""
Extractor of fields from scanned construction permit PDFs.
Stage 1: glm-ocr recognizes text and tables from each page.
Stage 2: llama3.1:8b extracts structured fields from OCR text into JSON.
"""

import sys
import json

from src.extractor import extract_fields


def main():
    if len(sys.argv) != 2:
        print("Использование: python main.py <путь_к_pdf>", file=sys.stderr)
        sys.exit(1)

    pdf_path = sys.argv[1]

    try:
        result = extract_fields(pdf_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except ConnectionError:
        print(
            "Ошибка: не удалось подключиться к ollama. "
            "Убедитесь, что ollama запущен (`ollama serve`).",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as e:
        import traceback
        print(f"Ошибка: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
