from html.parser import HTMLParser


class _TableTextExtractor(HTMLParser):
    """Convert HTML tables (as produced by glm-ocr) to readable plain text.

    Text outside <table> tags (section headers, field labels, annotations) is
    preserved — previously it was silently dropped because handle_data only
    stored content when _in_cell was True.
    """

    def __init__(self):
        super().__init__()
        self._lines: list[str] = []
        self._current_row: list[str] = []
        self._current_cell: list[str] = []
        self._in_cell: bool = False
        self._in_table: bool = False
        self._pending_text: list[str] = []  # text outside <table> tags

    def handle_starttag(self, tag: str, attrs):
        if tag == "table":
            self._flush_pending_text()
            self._in_table = True
        elif tag in ("td", "th") and self._in_table:
            self._in_cell = True
            self._current_cell = []

    def handle_endtag(self, tag: str):
        if tag in ("td", "th") and self._in_table:
            self._in_cell = False
            text = " ".join(self._current_cell).strip()
            self._current_row.append(text)
        elif tag == "tr":
            self._flush_row()
        elif tag == "table":
            self._in_table = False
            if self._lines and self._lines[-1] != "":
                self._lines.append("")

    def handle_data(self, data: str):
        if self._in_cell:
            stripped = data.strip()
            if stripped:
                self._current_cell.append(stripped)
        elif not self._in_table:
            stripped = data.strip()
            if stripped:
                self._pending_text.append(stripped)

    def _flush_pending_text(self):
        if self._pending_text:
            self._lines.extend(self._pending_text)
            self._pending_text = []

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
        self._flush_pending_text()
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
