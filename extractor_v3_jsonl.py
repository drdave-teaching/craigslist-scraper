# extractor_v3_jsonl.py
# Purpose: Convert TXT -> one-line JSON records and write as .jsonl files to GCS.
# Notes:
#   - Each output file contains exactly ONE line (valid NDJSON).
#   - Destination: gs://<bucket>/<PREFIX>/<run_id>/jsonl/<post_id>.jsonl
#   - Adds 'run_id' and 'scraped_at' (UTC ISO8601) to each record.

import os, re, json, logging, traceback
from datetime import datetime, timezone
from flask import Request, jsonify
from google.api_core import retry as gax_retry
from google.cloud import storage

# -------------------- ENV --------------------
PROJECT_ID  = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET")
PREFIX      = os.getenv("OUTPUT_PREFIX", "craigslist")   # e.g., "craigslist"

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

storage_client = storage.Client()

# -------------------- SIMPLE REGEX --------------------
PRICE_RE       = re.compile(r"\$\s?([0-9,]+)")
YEAR_RE        = re.compile(r"\b(19|20)\d{2}\b")
MAKE_MODEL_RE  = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][A-Za-z0-9]+)")

# -------------------- HELPERS --------------------
def _list_run_ids(bucket: str, prefix: str) -> list[str]:
    """Return sorted run_ids under gs://bucket/prefix/<run_id>/"""
    it = storage_client.list_blobs(bucket, prefix=f"{prefix}/", delimiter="/")
    # Force an iteration so it.prefixes is populated
    for _ in it:
        pass
    run_ids = []
    for p in sorted(getattr(it, "prefixes", [])):
        parts = p.strip("/").split("/")
        if len(parts) == 2 and parts[0] == prefix:
            run_ids.append(parts[1])
    return run_ids

def list_txt_for_run(run_id: str):
    prefix = f"{PREFIX}/{run_id}/txt/"
    bucket = storage_client.bucket(BUCKET_NAME)
    return [b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".txt")]

def download_text(blob_name: str) -> str:
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(retry=READ_RETRY, timeout=120)

def upload_jsonl_line(blob_name: str, record: dict):
    """Write exactly ONE line of JSON to a .jsonl object (valid NDJSON)."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    # Content-Type can be application/x-ndjson or application/json; both OK.
    blob.upload_from_string(line, content_type="application/x-ndjson")

def parse_run_id_as_ts(run_id: str) -> str:
    """
    Try to parse run_id like '20251021T090008Z' -> ISO8601 '2025-10-21T09:00:08Z'.
    If parsing fails, return current UTC time.
    """
    try:
        dt = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# -------------------- PARSE --------------------
def parse_listing(text: str) -> dict:
    """Extract simple fields from raw text."""
    d = {}

    # price
    m = PRICE_RE.search(text)
    if m:
        try:
            d["price"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    # year
    y = YEAR_RE.search(text)
    if y:
        try:
            d["year"] = int(y.group(0))
        except ValueError:
            pass

    # make & model (first two capitalized words after year line)
    mm = MAKE_MODEL_RE.search(text)
    if mm:
        d["make"] = mm.group(1)
        d["model"] = mm.group(2)

    # mileage: handle "Mileage: 144700", "odometer: 95,000", "120k miles", "144,700 miles"
    mi = None

    # (1) direct field
    m1 = re.search(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", text, re.I)
    if m1:
        try:
            mi = int(m1.group(1).replace(",", ""))
        except ValueError:
            mi = None

    # (2) shorthand "k"
    if mi is None:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", text, re.I)
        if m2:
            try:
                mi = int(float(m2.group(1)) * 1000)
            except ValueError:
                mi = None

    # (3) plain numeric followed by miles/mi
    if mi is None:
        m3 = re.search(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", text, re.I)
        if m3:
            try:
                mi = int(re.sub(r"[^\d]", "", m3.group(1)))
            except ValueError:
                mi = None

    if mi is not None:
        d["mileage"] = mi

    return d

# -------------------- HTTP ENTRY --------------------
def extract_http(request: Request):
    """
    v3 (JSONL): reads TXT listings for a run_id and writes ONE-LINE JSON records
    to gs://<bucket>/<PREFIX>/<run_id>/jsonl/<post_id>.jsonl
    """
    logging.getLogger().setLevel(logging.INFO)

    if not BUCKET_NAME:
        return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    # Request options
    run_id    = body.get("run_id")
    max_files = int(body.get("max_files") or 0)   # 0 = no limit
    overwrite = bool(body.get("overwrite") or False)  # kept for future use

    # If run_id not provided, pick newest
    if not run_id:
        runs = _list_run_ids(BUCKET_NAME, PREFIX)
        if not runs:
            return jsonify({"ok": False, "error": "no runs found under prefix", "prefix": PREFIX}), 200
        run_id = runs[-1]

    # Use run_id timestamp (fallback to now)
    scraped_at_iso = parse_run_id_as_ts(run_id)

    txt_blobs = list_txt_for_run(run_id)
    if max_files > 0:
        txt_blobs = txt_blobs[:max_files]

    processed, written, skipped, errors = 0, 0, 0, 0

    for name in txt_blobs:
        try:
            text = download_text(name)
            fields = parse_listing(text)

            # derive post_id directly from the txt filename
            txt_filename = os.path.basename(name)       # e.g. "123456789.txt"
            post_id = os.path.splitext(txt_filename)[0] # e.g. "123456789"

            # Build record (include provenance)
            record = {
                "post_id": post_id,
                "run_id": run_id,
                "scraped_at": scraped_at_iso,
                "source_txt": name,
                **fields,
            }

            out_key = f"{PREFIX}/{run_id}/jsonl/{post_id}.jsonl"

            # If not overwriting and destination exists, skip
            if not overwrite:
                bucket = storage_client.bucket(BUCKET_NAME)
                if bucket.blob(out_key).exists():
                    skipped += 1
                    processed += 1
                    continue

            upload_jsonl_line(out_key, record)
            written += 1

        except Exception as e:
            errors += 1
            logging.error(f"Failed {name}: {e}\n{traceback.format_exc()}")

        processed += 1

    result = {
        "ok": True,
        "version": "extractor-v3-jsonl",
        "run_id": run_id,
        "processed_txt": processed,
        "written_jsonl": written,
        "skipped_existing": skipped,
        "errors": errors
    }
    logging.info(json.dumps(result))
    return jsonify(result), 200
