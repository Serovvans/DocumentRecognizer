"""OCR-only test script: runs glm-ocr on all PDFs in data/разрешения/ and saves results to JSON.

Usage:
    python scripts/ocr_only.py [--output OUTPUT] [--workers N]

Output JSON format:
    {
        "path/to/file.pdf": {
            "pages": ["page 1 text", "page 2 text", ...],
            "combined": "=== Страница 1 ===\n...\n\n=== Страница 2 ===\n..."
        },
        ...
    }
    On error, the value is {"error": "message"}.
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from ollama import chat  # type: ignore[import]

from src.config import OCR_MODEL, OCR_PAGE_WORKERS
from src.pdf_utils import pdf_to_images_base64
from src.prompt import build_ocr_prompt

_DATA_DIR = Path(__file__).parent.parent / "data" / "разрешения"

_OCR_OPTIONS = {
    "temperature": 0,
    "num_batch": 2048,
    "num_predict": 3072,
}


def _ocr_page(image_b64: str, page_num: int, pdf_path: str) -> tuple[int, str]:
    print(f"  [{pdf_path}] страница {page_num}...", file=sys.stderr)
    response = chat(
        model=OCR_MODEL,
        messages=[
            {
                "role": "user",
                "content": build_ocr_prompt(),
                "images": [image_b64],
            }
        ],
        options=_OCR_OPTIONS,
    )
    return page_num, response.message.content or ""


def ocr_pdf(pdf_path: Path) -> dict:
    rel = str(pdf_path.relative_to(Path(__file__).parent.parent))
    print(f"[{rel}] конвертация в изображения...", file=sys.stderr)
    images = pdf_to_images_base64(str(pdf_path))

    print(f"[{rel}] OCR {len(images)} стр. моделью {OCR_MODEL}...", file=sys.stderr)
    page_texts: dict[int, str] = {}

    with ThreadPoolExecutor(max_workers=OCR_PAGE_WORKERS) as pool:
        futures = {
            pool.submit(_ocr_page, img, i, rel): i
            for i, img in enumerate(images, start=1)
        }
        for future in as_completed(futures):
            page_num, text = future.result()
            page_texts[page_num] = text

    pages = [page_texts[i] for i in sorted(page_texts)]
    combined = "\n\n".join(
        f"=== Страница {i} ===\n{text}" for i, text in enumerate(pages, start=1)
    )
    print(f"[{rel}] готово ({len(pages)} стр.)", file=sys.stderr)
    return {"pages": pages, "combined": combined}


def main() -> None:
    parser = argparse.ArgumentParser(description="OCR-only test: glm-ocr на всех PDF из data/разрешения/")
    parser.add_argument("--output", "-o", default="ocr_results.json", help="Выходной JSON-файл (default: ocr_results.json)")
    parser.add_argument("--workers", "-w", type=int, default=2, help="Число параллельных документов (default: 2)")
    args = parser.parse_args()

    pdf_files = sorted(_DATA_DIR.rglob("*.pdf"))
    if not pdf_files:
        print(f"Нет PDF в {_DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"Найдено {len(pdf_files)} PDF-файлов в {_DATA_DIR}", file=sys.stderr)

    results: dict[str, object] = {}

    def _process(pdf_path: Path) -> tuple[str, object]:
        rel = str(pdf_path.relative_to(Path(__file__).parent.parent))
        try:
            return rel, ocr_pdf(pdf_path)
        except Exception as exc:
            print(f"[{rel}] ОШИБКА: {exc}", file=sys.stderr)
            return rel, {"error": str(exc)}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_process, p): p for p in pdf_files}
        for future in as_completed(futures):
            key, value = future.result()
            results[key] = value

    output_path = Path(args.output)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nГотово. Результаты сохранены в {output_path} ({len(results)} документов)", file=sys.stderr)


if __name__ == "__main__":
    main()
