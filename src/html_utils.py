import re


def html_tables_to_text(html: str) -> str:
    """Convert HTML table markup from glm-ocr to plain text.

    Cells are separated by " | ", rows by newlines. All other tags are stripped.
    Handles malformed HTML and preserves every text node including content
    between tags that a state-machine parser would silently drop.
    """
    text = re.sub(r'</t[dh]\s*>', ' | ', html, flags=re.IGNORECASE)
    text = re.sub(r'</tr\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    lines = []
    for line in text.splitlines():
        line = ' '.join(line.split()).strip(' |')
        if line:
            lines.append(line)
    return '\n'.join(lines)
