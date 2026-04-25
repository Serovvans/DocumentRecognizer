import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

from app.config import OCR_MODEL, EXTRACTION_MODEL
from src.extractor import extract_fields_dynamic, DocumentRejected


def process_documents(
    pdf_paths: list[str],
    fields: list[dict],
    workers: int,
    callback: Callable[[dict], None],
    output_path: str,
    db_writer=None,
    classification_prompt: str = "",
) -> None:
    """
    Process *pdf_paths* in parallel, stream progress via *callback*, write
    JSONL results to *output_path*, and optionally insert rows via *db_writer*.
    """
    total = len(pdf_paths)

    if total == 0:
        callback(
            {
                "type": "complete",
                "total": 0,
                "successful": 0,
                "failed": 0,
                "rejected": 0,
                "error_files": [],
                "db_stats": None,
            }
        )
        return

    done = 0
    successful = 0
    failed = 0
    rejected = 0
    error_files: list[dict] = []
    start_time = time.monotonic()
    counter_lock = threading.Lock()

    field_names = [f["name"] for f in fields]

    # Return value: (path, result_or_None, error_or_None, rejection_reason_or_None)
    def _process_one(pdf_path: str) -> tuple[str, dict | None, str | None, str | None]:
        try:
            result = extract_fields_dynamic(
                pdf_path,
                fields,
                ocr_model=OCR_MODEL,
                extraction_model=EXTRACTION_MODEL,
                classification_prompt=classification_prompt,
            )
            # Treat fully-null results as failures (document has no useful data)
            if all(result.get(name) is None for name in field_names):
                return pdf_path, None, "Документ не содержит нужных данных", None
            return pdf_path, result, None, None
        except DocumentRejected as exc:
            return pdf_path, None, None, exc.reason
        except Exception as exc:
            return pdf_path, None, str(exc), None

    with open(output_path, "w", encoding="utf-8") as out:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {executor.submit(_process_one, p): p for p in pdf_paths}

            for future in as_completed(futures):
                pdf_path, result, error, rejection_reason = future.result()

                with counter_lock:
                    done += 1
                    elapsed = time.monotonic() - start_time
                    speed = elapsed / done
                    eta = (total - done) * speed

                    if rejection_reason is not None:
                        rejected += 1
                    elif error:
                        failed += 1
                        error_files.append({"file": pdf_path, "error": error})
                    else:
                        successful += 1

                # Write result line immediately (flush keeps memory bounded)
                if rejection_reason is not None:
                    record = {"file": pdf_path, "status": "rejected", "reason": rejection_reason}
                elif error:
                    record = {"file": pdf_path, "status": "error", "error": error}
                else:
                    record = {"file": pdf_path, "status": "ok", "data": result}
                    if db_writer:
                        try:
                            db_writer.write(pdf_path, result)
                        except Exception as db_exc:
                            record["db_error"] = str(db_exc)

                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()

                callback(
                    {
                        "type": "progress",
                        "total": total,
                        "done": done,
                        "successful": successful,
                        "failed": failed,
                        "rejected": rejected,
                        "last_file": Path(pdf_path).name,
                        "last_success": error is None and rejection_reason is None,
                        "last_error": error,
                        "last_rejected": rejection_reason,
                        "speed": round(speed, 1),
                        "eta": round(eta),
                    }
                )

    db_stats = None
    if db_writer:
        db_stats = {"inserted": db_writer.inserted, "errors": db_writer.errors}

    callback(
        {
            "type": "complete",
            "total": total,
            "successful": successful,
            "failed": failed,
            "rejected": rejected,
            "error_files": error_files,
            "db_stats": db_stats,
        }
    )
