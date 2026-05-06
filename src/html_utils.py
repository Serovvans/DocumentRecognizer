from html.parser import HTMLParser


class _TableTextExtractor(HTMLParser):
    """Convert HTML tables (as produced by glm-ocr) to readable plain text."""

    def __init__(self):
        super().__init__()
        self._lines: list[str] = []
        self._current_row: list[str] = []
        self._current_cell: list[str] = []
        self._in_cell: bool = False
        self._cell_is_header: bool = False

    def handle_starttag(self, tag: str, attrs):
        if tag in ("td", "th"):
            self._in_cell = True
            self._cell_is_header = tag == "th"
            self._current_cell = []

    def handle_endtag(self, tag: str):
        if tag in ("td", "th"):
            self._in_cell = False
            text = " ".join(self._current_cell).strip()
            self._current_row.append(text)
        elif tag == "tr":
            self._flush_row()
        elif tag == "table":
            if self._lines and self._lines[-1] != "":
                self._lines.append("")

    def handle_data(self, data: str):
        if self._in_cell:
            stripped = data.strip()
            if stripped:
                self._current_cell.append(stripped)

    def _flush_row(self):
        row = [c for c in self._current_row if c]
        self._current_row = []
        if not row:
            return
        if len(row) == 1:
            # Single cell — section header or standalone label
            self._lines.append(row[0])
        elif len(row) == 2:
            label, value = row[0], row[1]
            # Avoid double colon when label already ends with ":"
            sep = " " if label.endswith(":") else ": "
            self._lines.append(f"{label}{sep}{value}")
        else:
            # Three or more cells — join with tab
            self._lines.append("\t".join(row))

    def result(self) -> str:
        return "\n".join(self._lines)


def html_tables_to_text(html: str) -> str:
    """Convert HTML table output from glm-ocr to plain text.

    Preserves section headers and label/value pairs so the extraction LLM can
    reason about document structure without parsing HTML tags.
    """
    # Process page-by-page, keeping the === Страница N === separators
    parts: list[str] = []
    for chunk in html.split("\n\n"):
        if chunk.startswith("=== Страница"):
            # Find separator line and the content after it
            newline_pos = chunk.find("\n")
            if newline_pos == -1:
                parts.append(chunk)
                continue
            separator = chunk[:newline_pos]
            content = chunk[newline_pos + 1:]
            extractor = _TableTextExtractor()
            extractor.feed(content)
            text = extractor.result().strip()
            if text:
                parts.append(f"{separator}\n{text}")
            else:
                parts.append(separator)
        else:
            # Non-table chunk (shouldn't normally happen, but pass through)
            extractor = _TableTextExtractor()
            extractor.feed(chunk)
            text = extractor.result().strip()
            parts.append(text if text else chunk)

    return "\n\n".join(parts)
