import sys
import base64

import fitz  # pymupdf


def pdf_to_images_base64(pdf_path: str, dpi_scale: float = 2.5) -> list[str]:
    """Convert each PDF page to a base64-encoded PNG string."""
    doc = fitz.open(pdf_path)
    images = []
    for page_num, page in enumerate(doc):
        mat = fitz.Matrix(dpi_scale, dpi_scale)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
        print(f"  Страница {page_num + 1}/{len(doc)} обработана", file=sys.stderr)
    doc.close()
    return images
