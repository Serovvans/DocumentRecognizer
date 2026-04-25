import json
import re


def extract_json(text: str) -> dict:
    """Extract a JSON object from a model response string."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    match = re.search(r"\{[\s\S]+\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Не удалось извлечь JSON из ответа модели:\n{text}")


def get_last_parse_error(text: str) -> str:
    """Return the specific JSONDecodeError message for the JSON content found in text."""
    candidates = [text]

    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        candidates.append(m.group(1))

    m = re.search(r"\{[\s\S]+\}", text)
    if m:
        candidates.append(m.group(0))

    last_error = "не найден JSON-объект в ответе"
    for candidate in candidates:
        try:
            json.loads(candidate)
            return ""
        except json.JSONDecodeError as e:
            last_error = str(e)

    return last_error
