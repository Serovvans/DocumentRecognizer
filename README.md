# Document Recognizer

Веб-приложение для пакетного распознавания полей из отсканированных PDF-документов с помощью EasyOCR и локальных моделей Ollama.

**Конвейер на документ:** OCR каждой страницы (`EasyOCR`, языки `ru`+`en`) → извлечение полей (`qwen2.5:14b`) → JSON / PostgreSQL.

---

## Возможности

- Рекурсивный обход папки с PDF-файлами
- Настраиваемые поля: название + описание (откуда брать)
- Пресеты полей — сохранение и переиспользование
- Параллельная обработка (до 8 потоков)
- Прогресс в реальном времени: скорость (сек/файл) и оставшееся время
- Опциональная запись в PostgreSQL через SSH-туннель
- Скачивание итогового JSON (потоковый, не грузит RAM)
- Краткий отчёт: успехи, ошибки, статистика БД

---

## Требования

| Компонент | Версия |
|-----------|--------|
| Python | 3.11+ |
| [Ollama](https://ollama.com) | последняя |
| Модель `qwen2.5:14b` | `ollama pull qwen2.5:14b-instruct-q4_K_M` |
| EasyOCR | устанавливается через `requirements.txt` |
| PyTorch (CUDA) | устанавливается отдельно (см. ниже) |

---

## Установка

### 1. Установить PyTorch с поддержкой GPU (до `pip install -r requirements.txt`)

```bash
# CUDA 12.1 (RTX 30xx / 40xx и новее):
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121

# CUDA 11.8 (старые карты):
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118

# CPU only (очень медленно, не рекомендуется):
pip install torch torchvision
```

### 2. Установить зависимости и запустить

#### macOS / Linux

```bash
cd DocumentRecognizer_v1

python3 -m venv .venv
source .venv/bin/activate

# Сначала PyTorch (см. выше), затем:
pip install -r requirements.txt

cp .env.example .env
# отредактируйте .env — укажите SSH и БД реквизиты (если нужна запись в БД)
```

#### Windows

```powershell
cd DocumentRecognizer_v1

python -m venv .venv
.venv\Scripts\activate

# Сначала PyTorch (см. выше), затем:
pip install -r requirements.txt

copy .env.example .env
# откройте .env в редакторе и заполните реквизиты
```

### 3. Запустить Ollama с моделью извлечения

```bash
ollama serve
ollama pull qwen2.5:14b-instruct-q4_K_M
```

### 4. Проверить EasyOCR

```bash
python -c "import easyocr; r = easyocr.Reader(['ru','en'], gpu=True); print('EasyOCR OK')"
```

---

## Конфигурация (`.env`)

```dotenv
# SSH-туннель до сервера с PostgreSQL
SSH_HOST=192.168.25.15
SSH_PORT=22
SSH_USERNAME=root
SSH_PASSWORD=secret
# SSH_KEY_FILE=/path/to/key.pem   # альтернатива паролю

# PostgreSQL (адрес внутри сети за SSH)
DB_HOST=192.168.25.15
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=secret

# Ollama (только для этапа извлечения — OCR работает через EasyOCR/PyTorch)
OLLAMA_BASE_URL=http://localhost:11434
EXTRACTION_MODEL=qwen2.5:14b-instruct-q4_K_M

# EasyOCR (этап OCR)
EASYOCR_LANGUAGES=ru,en
EASYOCR_GPU=true
EASYOCR_CONFIDENCE_THRESHOLD=0.3
```

Если запись в БД не нужна, файл `.env` можно оставить пустым — приложение работает без него.

---

## Запуск

```bash
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

uvicorn app.main:app --reload --host 0.0.0.0 --port 8007
```

Откройте браузер: **http://localhost:8007**

---

## Использование

### Шаг 1 — Настройка

1. **Папка с документами** — введите путь к папке, нажмите **Сканировать**.  
   Приложение рекурсивно найдёт все `.pdf` файлы.

2. **Поля для извлечения** — добавьте строки с:
   - **Название поля** — ключ в итоговом JSON (например, `Дата разрешения`)
   - **Описание** — подсказка модели (например, `Дата выдачи разрешения на строительство`)

3. **Пресеты** — сохраните набор полей под именем, чтобы переиспользовать позже.

4. **Параллельных потоков** — 1–8. EasyOCR использует один общий Reader на GPU, поэтому 2–4 потока дают хороший выигрыш.

5. **База данных** (опционально) — включите переключатель, укажите схему и таблицу.  
   Таблица должна существовать; недостающие колонки добавляются автоматически.  
   Все учётные данные берутся из `.env`.

### Шаг 2 — Обработка

Нажмите **Начать обработку**. Вы увидите:
- Счётчики: документов, успешных, ошибочных
- Прогресс-бар с процентом
- Скорость (сек/файл) и оставшееся время
- Лог с результатом каждого файла в реальном времени

### Шаг 3 — Результаты

По завершении открывается страница с отчётом:
- Итоговые цифры и точность
- Список файлов с ошибками (сворачивается)
- Статистика записи в БД (если была включена)
- Кнопка **Скачать JSON** — файл-массив со всеми результатами

---

## Формат итогового JSON

```json
[
  {
    "file": "/path/to/doc.pdf",
    "status": "ok",
    "data": {
      "Дата разрешения": "15.03.2023",
      "Номер разрешения": "77-123456"
    }
  },
  {
    "file": "/path/to/broken.pdf",
    "status": "error",
    "error": "Документ не содержит нужных данных"
  }
]
```

Значение поля может быть строкой, массивом строк (несколько вхождений) или `null`.

---

## CLI (оригинальный режим)

```bash
python main.py data/doc1.pdf data/doc2.pdf
```

Поля берутся из `src/config.py`, вывод — в stdout (JSON).

---

## Структура проекта

```
DocumentRecognizer_v1/
├── app/                    # Веб-приложение
│   ├── main.py             # FastAPI точка входа
│   ├── config.py           # Настройки из .env
│   ├── routers/
│   │   ├── presets.py      # CRUD пресетов
│   │   ├── process.py      # WebSocket + /scan
│   │   └── export.py       # Скачивание JSON
│   ├── services/
│   │   ├── scanner.py      # Поиск PDF
│   │   ├── processor.py    # Оркестрация обработки
│   │   └── db_writer.py    # Запись в PostgreSQL
│   └── static/             # Фронтенд (HTML/CSS/JS)
├── src/                    # Ядро pipeline (OCR + extraction)
│   ├── extractor.py        # EasyOCR + реконструкция разметки + LLM извлечение
│   ├── pdf_utils.py        # PDF → numpy array (EasyOCR) / base64 (превью)
│   ├── prompt.py
│   ├── json_utils.py
│   └── config.py
├── presets/                # Сохранённые пресеты (JSON)
├── output/                 # Временные результаты сессий (JSONL)
├── main.py                 # Оригинальный CLI
├── requirements.txt
├── .env.example
└── README.md
```

---

## Настройка качества OCR

В `src/extractor.py` функция `_reconstruct_layout` имеет два параметра для тонкой настройки:

| Параметр | Умолч. | Назначение |
|----------|--------|-----------|
| `row_threshold` | `0.6 × median_h` | Допуск по вертикали для объединения в одну строку |
| `col_gap_threshold` | `0.04 × page_width` | Минимальный горизонтальный зазор между колонками таблицы |

Если таблицы распознаются как обычный текст — уменьшите `col_gap_threshold`. Если обычный текст разбивается на таблицы — увеличьте.

---

## Известные ограничения

- Скорость обработки зависит от GPU и выбранных моделей.  
  Типичное время: 15–60 с на документ при 4–6 страницах (EasyOCR + GPU значительно быстрее glm-ocr).
- Очень большие PDF (50+ страниц) могут занять много времени.
- EasyOCR загружает модель один раз при первом запросе (~300 МБ в VRAM); последующие страницы обрабатываются быстро.
- Для записи в БД таблица должна существовать заранее и иметь первичный ключ.
