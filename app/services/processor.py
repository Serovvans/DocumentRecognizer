import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

from app.config import OCR_MODEL, EFFECTIVE_EXTRACTION_MODEL, EXTRACT_WORKERS
from src.extractor import ocr_document, extract_fields_from_ocr, detect_and_split_permits, DocumentRejected


def process_documents(
    pdf_paths: list[str],
    fields: list[dict],
    workers: int,
    callback: Callable[[dict], None],
    output_path: str,
    db_writer=None,
    classification_prompt: str = "",
    per_field: bool = False,
    sections: list[dict] | None = None,
) -> None:
    """
    Two-phase processing: first OCR all documents in parallel, then run LLM
    extraction on each OCR result with limited concurrency.

    Progress events:
      {"type": "ocr_done",  "file": name, "ocr_done": N, "total": N}
      {"type": "progress",  ...}   — emitted after each extraction completes
      {"type": "complete",  ...}

    Failed documents (errors, not rejections) are retried once on the extraction
    phase — up to 2 attempts total. Rejected documents are never retried.
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

    output_stem = Path(output_path).stem

    # ── Phase 1: OCR ──────────────────────────────────────────────────────────
    ocr_results: dict[str, str | Exception] = {}
    ocr_workers = max(1, workers)
    ocr_done_count = 0

    def _ocr_one(path: str) -> tuple[str, str]:
        return path, ocr_document(path, ocr_model=OCR_MODEL)

    with ThreadPoolExecutor(max_workers=ocr_workers) as pool:
        ocr_futures = {pool.submit(_ocr_one, p): p for p in pdf_paths}
        for future in as_completed(ocr_futures):
            path = ocr_futures[future]
            try:
                _, text = future.result()
                ocr_results[path] = text
            except Exception as exc:
                ocr_results[path] = exc
            ocr_done_count += 1
            callback(
                {
                    "type": "ocr_done",
                    "file": Path(path).name,
                    "ocr_done": ocr_done_count,
                    "total": total,
                }
            )

    # Save OCR results to auxiliary file
    ocr_aux_path = str(Path(output_path).with_name(f"{output_stem}_ocr.json"))
    with open(ocr_aux_path, "w", encoding="utf-8") as f:
        json.dump(
            {p: t for p, t in ocr_results.items() if isinstance(t, str)},
            f,
            ensure_ascii=False,
            indent=2,
        )

    # ── Phase 2: LLM extraction ───────────────────────────────────────────────
    extract_concurrency = max(1, min(EXTRACT_WORKERS, workers))
    raw_responses: dict = {}
    raw_lock = threading.Lock()

    # Each element: (result, error, rejection_reason)
    _PermitResult = tuple[dict | None, str | None, str | None]

    def _extract_one(
        pdf_path: str, ocr_text: str
    ) -> tuple[str, list[_PermitResult]]:
        """Extract fields from *ocr_text*, splitting into individual permits when needed.

        Returns (pdf_path, list_of_permit_results) where each permit result is
        (result_dict | None, error_str | None, rejection_reason | None).
        """
        permit_texts = detect_and_split_permits(
            ocr_text,
            prefix=f"[{pdf_path}] ",
        )

        permit_results: list[_PermitResult] = []
        for permit_ocr in permit_texts:
            local_raw: dict = {}
            try:
                result = extract_fields_from_ocr(
                    permit_ocr,
                    fields,
                    extraction_model=EFFECTIVE_EXTRACTION_MODEL,
                    classification_prompt=classification_prompt,
                    per_field=per_field,
                    sections=sections or [],
                    prefix=f"[{pdf_path}] ",
                    raw_collector=local_raw,
                )
                with raw_lock:
                    raw_responses[pdf_path] = next(iter(local_raw.values()), None)
                permit_results.append((result, None, None))
            except DocumentRejected as exc:
                permit_results.append((None, None, exc.reason))
            except Exception as exc:
                permit_results.append((None, str(exc), None))

        return pdf_path, permit_results

    def _emit(
        out,
        pdf_path: str,
        result: dict | None,
        error: str | None,
        rejection_reason: str | None,
        permit_index: int | None = None,
        permits_total: int | None = None,
        count_done: bool = True,
    ) -> None:
        nonlocal done, successful, failed, rejected

        with counter_lock:
            # Increment file-level "done" counter only once per source file so
            # that progress % stays meaningful even for multi-permit documents.
            if count_done:
                done += 1
            elapsed = time.monotonic() - start_time
            speed = elapsed / max(done, 1)
            eta = (total - done) * speed

            if rejection_reason is not None:
                rejected += 1
            elif error:
                failed += 1
                error_files.append({"file": pdf_path, "error": error})
            else:
                successful += 1

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

        if permit_index is not None:
            record["permit_index"] = permit_index
            record["permits_total"] = permits_total

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

    with open(output_path, "w", encoding="utf-8") as out:
        with ThreadPoolExecutor(max_workers=extract_concurrency) as pool:
            # Immediately emit errors for documents whose OCR failed
            retry_paths: list[str] = []
            ok_ocr: dict[str, str] = {}
            for path, ocr_result in ocr_results.items():
                if isinstance(ocr_result, Exception):
                    _emit(out, path, None, f"Ошибка OCR: {ocr_result}", None)
                else:
                    ok_ocr[path] = ocr_result

            # Pass 1: extract all documents with successful OCR
            futures = {
                pool.submit(_extract_one, p, t): p for p, t in ok_ocr.items()
            }
            for future in as_completed(futures):
                pdf_path, permit_results = future.result()
                is_multi = len(permit_results) > 1
                # For single-permit failures, queue a retry; multi-permit failures
                # are emitted immediately (no retry to avoid re-running segmentation).
                if not is_multi and len(permit_results) == 1:
                    result, error, rejection = permit_results[0]
                    if error is not None:
                        retry_paths.append(pdf_path)
                    else:
                        _emit(out, pdf_path, result, error, rejection)
                else:
                    for idx, (result, error, rejection) in enumerate(permit_results):
                        _emit(out, pdf_path, result, error, rejection,
                              permit_index=idx, permits_total=len(permit_results),
                              count_done=(idx == 0))

            # Pass 2: one retry per single-permit document that errored during extraction
            if retry_paths:
                retry_futures = {
                    pool.submit(_extract_one, p, ok_ocr[p]): p for p in retry_paths
                }
                for future in as_completed(retry_futures):
                    pdf_path, permit_results = future.result()
                    result, error, rejection = permit_results[0]
                    _emit(out, pdf_path, result, error, rejection)

    # Save raw extraction responses to auxiliary file
    raw_aux_path = str(Path(output_path).with_name(f"{output_stem}_raw.json"))
    with open(raw_aux_path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, ensure_ascii=False, indent=2)

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
