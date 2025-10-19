import os, re, json, logging, traceback
from flask import Request, jsonify
from google.cloud import storage
from google.api_core import retry as gax_retry

# -------------------- ENV --------------------
PROJECT_ID  = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET")
PREFIX      = os.getenv("OUTPUT_PREFIX", "craigslist")

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

storage_client = storage.Client()

# -------------------- SIMPLE REGEX --------------------
PRICE_RE    = re.compile(r"\$\s?([0-9,]+)")
YEAR_RE     = re.compile(r"\b(19|20)\d{2}\b")
MILEAGE_RE  = re.compile(r"(\d{1,3}(?:,\d{3})+)\s*(?:mi|miles)\b", re.I)
MAKE_MODEL_RE = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][A-Za-z0-9]+)")

# -------------------- HELPERS --------------------
def list_txt_for_run(run_id: str):
    prefix = f"{PREFIX}/{run_id}/txt/"
    bucket = storage_client.bucket(BUCKET_NAME)
    return [b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".txt")]

def download_text(blob_name: str) -> str:
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(retry=READ_RETRY, timeout=120)

def upload_json(blob_name: str, data: dict):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")

# -------------------- PARSE --------------------
def parse_listing(text: str) -> dict:
    """Extract simple fields from raw text."""
    d = {}

    # price
    m = PRICE_RE.search(text)
    if m:
        d["price"] = int(m.group(1).replace(",", ""))

    # year
    y = YEAR_RE.search(text)
    if y:
        d["year"] = int(y.group(0))

    # make & model (first two capitalized words after year line)
    mm = MAKE_MODEL_RE.search(text)
    if mm:
        d["make"] = mm.group(1)
        d["model"] = mm.group(2)

    # mileage
    mi = MILEAGE_RE.search(text)
    if mi:
        d["mileage"] = int(mi.group(1).replace(",", ""))

    return d

# -------------------- HTTP ENTRY --------------------
def extract_http(request: Request):
    logging.getLogger().setLevel(logging.INFO)
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    run_id = body.get("run_id")
    if not run_id:
        return jsonify({"ok": False, "error": "missing run_id"}), 400

    txt_blobs = list_txt_for_run(run_id)
    processed, written, errors = 0, 0, 0

    for name in txt_blobs:
        try:
            text = download_text(name)
            fields = parse_listing(text)
            fields["source_txt"] = name

            # derive post_id from filename
            post_id = re.search(r"(\d{6,12})", name)
            if post_id:
                fields["post_id"] = post_id.group(1)
            else:
                fields["post_id"] = os.path.basename(name)

            out_key = f"{PREFIX}/{run_id}/structured/json2/{fields['post_id']}.json"
            upload_json(out_key, fields)
            written += 1
        except Exception as e:
            errors += 1
            logging.error(f"Failed {name}: {e}\n{traceback.format_exc()}")
        processed += 1

    result = {"ok": True, "run_id": run_id, "processed": processed, "written_json": written, "errors": errors}
    logging.info(json.dumps(result))
    return jsonify(result), 200
