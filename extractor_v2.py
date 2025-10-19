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

    # mileage: capture values like "Mileage: 144700", "144,700 miles", "120k mi", or "odometer: 95000"
    mi = None

    # 1️⃣ direct field form (Mileage: 144700, Odometer: 95000)
    m1 = re.search(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", text, re.I)
    if m1:
        try:
            mi = int(m1.group(1).replace(",", ""))
        except ValueError:
            pass

    # 2️⃣ shorthand form (e.g., "120k miles")
    if mi is None:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", text, re.I)
        if m2:
            try:
                mi = int(float(m2.group(1)) * 1000)
            except ValueError:
                pass

    # 3️⃣ plain numeric form followed by "miles" or "mi"
    if mi is None:
        m3 = re.search(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", text, re.I)
        if m3:
            try:
                mi = int(re.sub(r"[^\d]", "", m3.group(1)))
            except ValueError:
                pass

    if mi is not None:
        d["mileage"] = mi

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

            # derive post_id directly from the txt filename
            txt_filename = os.path.basename(name)              # e.g. "123456789.txt"
            post_id = os.path.splitext(txt_filename)[0]         # e.g. "123456789"
            fields["post_id"] = post_id


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
