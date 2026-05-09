import sys
import base64

import fitz  # pymupdf

# glm-ocr crashes with GGML_ASSERT when image tiles exceed model capacity.
# Capping the longest side at 1120px keeps tile count within safe bounds.
_MAX_IMAGE_PX = 1120


def pdf_to_images_base64(pdf_path: str, dpi_scale: float = 2.0) -> list[str]:
    """Convert each PDF page to a base64-encoded PNG string."""
    doc = fitz.open(pdf_path)
    images = []
    for page_num, page in enumerate(doc):
        rect = page.rect
        longest = max(rect.width, rect.height)
        scale = min(dpi_scale, _MAX_IMAGE_PX / longest) if longest > 0 else dpi_scale
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
        print(f"  Страница {page_num + 1}/{len(doc)} обработана", file=sys.stderr)
    doc.close()
    return images
