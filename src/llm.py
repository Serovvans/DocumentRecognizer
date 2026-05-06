"""Backend-agnostic text model calls.

OCR (vision) always stays on Ollama; only text extraction/classification
is dispatched here based on EXTRACTION_BACKEND.
"""

import os
import threading
from typing import Callable

from dotenv import load_dotenv

load_dotenv()

EXTRACTION_BACKEND: str = os.getenv("EXTRACTION_BACKEND", "ollama")
_GIGACHAT_CREDENTIALS: str = os.getenv("GIGACHAT_CREDENTIALS", "")

# Per-thread GigaChat instances to avoid shared-state concurrency issues
_gigachat_local = threading.local()

# Serialise all GigaChat network calls: the library's token-refresh logic is
# not thread-safe and concurrent invoke() calls on the same (or different)
# instances cause indefinite hangs.
_gigachat_call_lock = threading.Lock()


def call_text_model(
    messages: list[dict],
    model: str,
    log: Callable[[str], None] | None = None,
    max_tokens: int = 2048,
) -> str:
    """Call the configured text backend and return the response string.

    messages: OpenAI-style [{"role": "system"|"user"|"assistant", "content": "..."}]
    model:    Ollama model name when backend=ollama; GigaChat model name when backend=gigachat
    """
    if EXTRACTION_BACKEND == "gigachat":
        return _call_gigachat(messages, model, log)
    return _call_ollama(messages, model, log, max_tokens)


def _call_ollama(
    messages: list[dict],
    model: str,
    log: Callable[[str], None] | None,
    max_tokens: int,
) -> str:
    from ollama import chat  # type: ignore[import]

    response = chat(
        model=model,
        messages=messages,
        options={
            "temperature": 0,
            "num_ctx": 8192,
            "num_batch": 512,
            "num_predict": max_tokens,
            "num_gpu": 99,
        },
    )
    text: str = response.message.content or ""
    if not text.strip() and getattr(response.message, "thinking", None):
        if log:
            log("content пустой, используем thinking как fallback")
        text = response.message.thinking or ""
    return text


def _get_gigachat_instance(model: str):
    """Return a per-thread GigaChat instance, creating it if needed."""
    instance = getattr(_gigachat_local, "instance", None)
    instance_model = getattr(_gigachat_local, "model", "")
    if instance is None or instance_model != model:
        from langchain_community.chat_models import GigaChat  # type: ignore[import]

        _gigachat_local.instance = GigaChat(
            model=model,
            credentials=_GIGACHAT_CREDENTIALS,
            verify_ssl_certs=False,
            temperature=0,
        )
        _gigachat_local.model = model
    return _gigachat_local.instance


def _call_gigachat(
    messages: list[dict],
    model: str,
    log: Callable[[str], None] | None = None,
) -> str:
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage  # type: ignore[import]

    lc_messages = []
    for m in messages:
        role, content = m["role"], m["content"]
        if role == "system":
            lc_messages.append(SystemMessage(content=content))
        elif role == "user":
            lc_messages.append(HumanMessage(content=content))
        elif role == "assistant":
            lc_messages.append(AIMessage(content=content))

    llm = _get_gigachat_instance(model)
    try:
        with _gigachat_call_lock:
            response = llm.invoke(lc_messages)
        return response.content or ""
    except Exception as exc:
        if log:
            log(f"[GigaChat] Ошибка вызова: {exc}")
        raise
