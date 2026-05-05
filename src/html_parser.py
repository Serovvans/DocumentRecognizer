"""Direct field extraction from glm-ocr HTML table output.

glm-ocr returns structured HTML tables where each row has a label cell
(e.g. "2.2.1. Полное наименование:") and a value cell.  For modern Russian
construction-permit forms (форма 2017+, Приказ Минстроя) the section/field
numbers are stable, so we can map them directly to output field names without
an LLM call.

The LABEL_TO_FIELD mapping covers the standard numbered sections.  Old-format
permits (pre-2017, free-text layout) produce no numbered labels and will yield
an empty extraction — the caller should fall back to LLM in that case.
"""

import re
from html.parser import HTMLParser


# Maps section label prefix → output field name.
# Both "2.2.1." and "2.2.1" styles are matched (trailing dot optional).
# Special _-prefixed names are internal: assembled into a final field by
# _assemble_builder().
LABEL_TO_FIELD: dict[str, str] = {
    # Раздел 1 — Реквизиты разрешения
    "1.1": "rns_date",
    "1.2": "rns_number",
    "1.3": "org_name",
    "1.4": "end_date",
    "1.5": "correction_date",
    # Раздел 2 — Застройщик (физлицо/ИП)
    "2.1.1": "_builder_last",
    "2.1.2": "_builder_first",
    "2.1.3": "_builder_mid",
    "2.1.4": "inn",
    "2.1.5": "ogrn",
    # Раздел 2 — Застройщик (юрлицо)
    "2.2.1": "builder",
    "2.2.2": "inn",
    "2.2.3": "ogrn",
    # Раздел 3 — Объект
    "3.1": "oks_name",
    "3.2": "type_work",
    "3.3.2": "municipal_name",
    # Раздел 4 — Земельный участок
    "4.1": "cad_num_zu",
    "4.2": "area_zu",
    # Раздел 7 — Характеристики
    "7.4": "build_area",
    "7.5": "area_oks",
}

# Regex that pulls the leading section number out of a label cell text.
# Matches patterns like "1.1.", "2.2.1.", "7.4.", "7.4.1." at the start.
_LABEL_RE = re.compile(r'^(\d+(?:\.\d+)+)\.?\s')


class _TableCellCollector(HTMLParser):
    """Collect <td> and <th> cell text in document order."""

    def __init__(self) -> None:
        super().__init__()
        self._in_cell = False
        self._depth = 0
        self._buf: list[str] = []
        self.cells: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in ("td", "th"):
            if self._depth == 0:
                self._buf = []
            self._depth += 1
            self._in_cell = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._depth > 0:
            self._depth -= 1
            if self._depth == 0:
                self.cells.append("".join(self._buf).strip())
                self._in_cell = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._buf.append(data)


def _extract_cells(html_fragment: str) -> list[str]:
    """Return all <td>/<th> text values from an HTML fragment."""
    parser = _TableCellCollector()
    parser.feed(html_fragment)
    return parser.cells


def _normalize_label(text: str) -> str | None:
    """Extract the dotted section number from a cell text, e.g. '2.2.1'."""
    m = _LABEL_RE.match(text.strip())
    if not m:
        return None
    # Normalise: strip trailing dot, collapse whitespace
    return m.group(1).rstrip(".")


def _assemble_builder(raw: dict[str, str]) -> str | None:
    """Combine _builder_last / _builder_first / _builder_mid into one string."""
    parts = [raw.get(k, "").strip() for k in ("_builder_last", "_builder_first", "_builder_mid")]
    joined = " ".join(p for p in parts if p)
    return joined if joined else None


def parse_ocr_html(ocr_text: str) -> dict[str, object]:
    """Parse glm-ocr HTML output and return a field→value dict.

    Values come directly from labeled table cells.  Only fields present in
    LABEL_TO_FIELD are returned; absent fields are omitted (caller decides
    the default).

    Multi-value fields (cad_num_zu, area_zu) accumulate into a list when the
    same label appears more than once.

    Returns an empty dict for old-format documents that have no numbered labels.
    """
    # Pull all HTML table fragments from the combined OCR text (which may
    # contain plain text between === Страница N === markers too).
    html_blocks = re.findall(r'<table[\s\S]*?</table>', ocr_text, re.IGNORECASE)
    if not html_blocks:
        return {}

    raw: dict[str, object] = {}
    # Track internal builder name parts separately
    builder_parts: dict[str, str] = {}
    # Track multi-value fields
    multi_fields = {"cad_num_zu", "area_zu"}

    for block in html_blocks:
        cells = _extract_cells(block)
        # Walk cells in pairs: label cell, value cell
        i = 0
        while i < len(cells) - 1:
            label_text = cells[i]
            value_text = cells[i + 1]
            label_key = _normalize_label(label_text)
            if label_key and label_key in LABEL_TO_FIELD:
                field = LABEL_TO_FIELD[label_key]
                value = value_text.strip()
                if field.startswith("_builder_"):
                    builder_parts[field] = value
                    i += 2
                    continue
                if field in multi_fields:
                    existing = raw.get(field)
                    if value:
                        if existing is None:
                            raw[field] = value
                        elif isinstance(existing, list):
                            existing.append(value)
                        else:
                            raw[field] = [existing, value]
                elif value:
                    # Later pages can overwrite earlier ones (e.g. correction_date
                    # may appear twice; second occurrence is the amended date).
                    # For single-value fields we keep the first non-empty value.
                    if field not in raw:
                        raw[field] = value
                i += 2
            else:
                i += 1

    # Assemble physical-person builder name if we collected name parts
    if builder_parts and "builder" not in raw:
        assembled = _assemble_builder(builder_parts)
        if assembled:
            raw["builder"] = assembled

    return raw


def is_modern_format(ocr_text: str) -> bool:
    """Return True if the OCR text looks like a modern (post-2017) structured form.

    Heuristic: presence of numbered section markers like "1.1.", "2.2.1.", etc.
    """
    return bool(re.search(r'\b\d+\.\d+\.?\s', ocr_text))
