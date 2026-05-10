import re

_HTML_ENTITIES = (
    ("&nbsp;", " "),
    ("&amp;", "&"),
    ("&lt;", "<"),
    ("&gt;", ">"),
    ("&quot;", '"'),
    ("&apos;", "'"),
)


def html_tables_to_text(html: str) -> str:
    """Convert HTML table markup from glm-ocr to plain text.

    Cells are separated by " | ", rows by newlines. All other tags are stripped.
    Handles malformed HTML and preserves every text node including content
    between tags that a state-machine parser would silently drop.
    """
    # Decode named and numeric HTML entities before tag processing
    for entity, char in _HTML_ENTITIES:
        html = html.replace(entity, char)
    html = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), html)
    # <br> inside cells → space so adjacent words don't merge
    html = re.sub(r'<br\s*/?>', ' ', html, flags=re.IGNORECASE)
    text = re.sub(r'</t[dh]\s*>', ' | ', html, flags=re.IGNORECASE)
    text = re.sub(r'</tr\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    lines = []
    for line in text.splitlines():
        line = ' '.join(line.split()).strip(' |')
        if line:
            lines.append(line)
    return '\n'.join(lines)
