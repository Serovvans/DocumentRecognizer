import os

from dotenv import load_dotenv

load_dotenv()

OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OCR_MODEL: str = os.getenv("OCR_MODEL", "deepseek-ocr")
EXTRACTION_MODEL: str = os.getenv("EXTRACTION_MODEL", "qwen2.5:14b-instruct-q4_K_M")

# "ollama" uses local Ollama models; "gigachat" uses GigaChat API via LangChain
EXTRACTION_BACKEND: str = os.getenv("EXTRACTION_BACKEND", "ollama")
GIGACHAT_MODEL: str = os.getenv("GIGACHAT_MODEL", "GigaChat-2")

# Resolved extraction model name passed to the LLM backend
EFFECTIVE_EXTRACTION_MODEL: str = (
    GIGACHAT_MODEL if EXTRACTION_BACKEND == "gigachat" else EXTRACTION_MODEL
)

# Число документов, обрабатываемых параллельно (фаза OCR)
MAX_WORKERS = 4

# Число документов, обрабатываемых параллельно на фазе извлечения (LLM).
# Для крупной модели рекомендуется 1, чтобы не делить VRAM между несколькими экземплярами.
EXTRACT_WORKERS: int = int(os.getenv("EXTRACT_WORKERS", "1"))

# Число страниц одного документа, OCR которых идёт параллельно.
# Ollama умеет очередить запросы к одной модели, поэтому 2–4 дают реальный выигрыш.
# Увеличивать сверх 4 смысла нет — GPU всё равно один.
OCR_PAGE_WORKERS = 4

# PDF page count above which we check for multiple permits inside one file.
# Typical single-permit documents are 4–5 pages; multi-permit files are 19–20.
MULTI_PERMIT_PAGE_THRESHOLD: int = int(os.getenv("MULTI_PERMIT_PAGE_THRESHOLD", "7"))

FIELDS = [
    "Наименование муниципального образования",
    "Дата разрешения на строительство",
    "Номер разрешения на строительство",
    "Наименование органа (организации)",
    "Срок действия настоящего разрешения",
    "Дата внесения изменений или исправлений",
    "Наименование застройщика/ФИО",
    "ИНН",
    "ОГРН",
    "Наименование объекта капитального строительства (этапа) в соответствии с проектной документацией",
    "Вид выполняемых работ в отношении объекта капитального строительства в соответствии с проектной документацией",
    "Площадь застройки",
    "Площадь кв.м.",
    "Кадастровый номер земельного участка (земельных участков), в границах которого (которых) расположен или планируется расположение объекта капитального строительства",
]
