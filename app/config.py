import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent

# SSH
SSH_HOST: str = os.getenv("SSH_HOST", "")
SSH_PORT: int = int(os.getenv("SSH_PORT", "22"))
SSH_USERNAME: str = os.getenv("SSH_USERNAME", "")
SSH_PASSWORD: str | None = os.getenv("SSH_PASSWORD") or None
SSH_KEY_FILE: str | None = os.getenv("SSH_KEY_FILE") or None

# PostgreSQL
DB_HOST: str = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
DB_NAME: str = os.getenv("DB_NAME", "postgres")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

# Ollama
OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OCR_MODEL: str = os.getenv("OCR_MODEL", "glm-ocr")
EXTRACTION_MODEL: str = os.getenv("EXTRACTION_MODEL", "llama3.1:8b")

# Paths
PRESETS_DIR: Path = BASE_DIR / "presets"
OUTPUT_DIR: Path = BASE_DIR / "output"
