# HTTP cron extractor for GCF Gen2 (no Eventarc).
# - Scans gs://$GCS_BUCKET/$OUTPUT_PREFIX/<run_id>/txt/*.txt
# - Writes JSON to .../<run_id>/structured/json/<post_id>.json
# - Keeps per-run "processed" state so cron jobs don't reprocess the same TXT
#
# Trigger:
#   Cloud Scheduler -> HTTP POST (optionally: {"run_id":"YYYYMMDDThhmmssZ"})
#
# Env:
#   PROJECT_ID      (e.g., "craigslist-scraper-v2")
#   LOCATION        (e.g., "us-central1")
#   GCS_BUCKET      (e.g., "craigslist-data-<project>")
#   OUTPUT_PREFIX   (default "craigslist")

import os, re, json, logging, traceback
from typing import Optional, List, Dict, Set

from flask import Request, jsonify
from google.cloud import storage
from google.api_core import retry as gax_retry
from pydantic import BaseModel, Field, validator

# -------------------- Config / Env --------------------

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION   = os.getenv("LOCATION", "us-central1")
BUCKET     = os.getenv("GCS_BUCKET")
PREFIX     = os.getenv("OUTPUT_PREFIX", "craigslist")

# Per-run processed state: gs://<bucket>/<PREFIX>/state/<run_id>.txt
STATE_PREFIX = f"{PREFIX}/state"

# -------------------- Globals --------------------

_SC: Optional[storage.Client] = None
_MODEL: Optional[GenerativeModel] = None

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

# Regex helpers
POST_ID_RE = re.compile(r"/([0-9]{8,12})\.html")
FNAME_ID_RE = re.compile(r"([0-9]{8,12})")
YEAR_RE     = re.compile(r"\b(19|20)\d{2}\b")
PRICE_LINE_RE = re.compile(r"\$?([\d,]+)")  # for "Price: $12,345" line
PRICE_FALLBACK_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:[, ]?[0-9]{3})+|[0-9]{3,6})\b")

# -------------------- Storage helpers --------------------

def _sc() -> storage.Client:
    global _SC
    if _SC is None:
        _SC = storage.Client()
    return _SC

def _b() -> storage.Bucket:
    return _sc().bucket(BUCKET)

def _download_text(key: str) -> str:
    bs = _b().blob(key).download_as_bytes(retry=READ_RETRY, timeout=120)
    return bs.decode("utf-8", errors="replace")

def _upload_json(key: str, data: dict):
    _b().blob(key).upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json"
    )

def _read_state(run_id: str) -> Set[str]:
    """Per-run set of processed TXT blob names."""
    key = f"{STATE_PREFIX}/{run_id}.txt"
    bl = _b().blob(key)
    if not bl.exists():
        return set()
    try:
        txt = bl.download_as_text()
        return set(line.strip() for line in txt.splitlines() if line.strip())
    except Exception:
        return set()

def _write_state(run_id: str, processed: Set[str]):
    key = f"{STATE_PREFIX}/{run_id}.txt"
    text = "\n".join(sorted(processed))
    _b().blob(key).upload_from_string(text, content_type="text/plain")

# -------------------- Vertex / schema --------------------

USE_VERTEX = os.getenv("USE_VERTEX", "1") == "1"

def _model():
    """Lazy Vertex init; only when called and only if USE_VERTEX=1."""
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    if not USE_VERTEX:
        raise RuntimeError("vertex_disabled")

    try:
        import vertexai  # provided by google-cloud-aiplatform
        from vertexai.preview.generative_models import GenerativeModel, Part
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        _MODEL = GenerativeModel("gemini-1.5-pro")
        _model.Part = Part  # stash for callers
        return _MODEL
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "vertexai_not_installed: add google-cloud-aiplatform to requirements.txt"
        ) from e



class Listing(BaseModel):
    post_id: str
    url: Optional[str]
    title: Optional[str]
    price: Optional[int] = Field(None, ge=0)
    year: Optional[int] = Field(None, ge=1950, le=2100)
    make: Optional[str]
    model: Optional[str]
    trim: Optional[str] = None
    mileage: Optional[int] = Field(None, ge=0)
    vin: Optional[str] = None
    color: Optional[str] = None
    transmission: Optional[str] = None
    condition: Optional[str] = None
    location: Optional[str] = None
    posted_iso: Optional[str] = None
    body: Optional[str] = None
    attrs_json: Optional[dict] = None

    @validator("vin")
    def _clean_vin(cls, v):
        if not v: return v
        return v.upper().replace(" ", "").replace("-", "")

def model_extract_json(text: str, url: Optional[str], post_id: str) -> Dict:
    system = (
        "Extract car listing data as STRICT JSON matching the schema. "
        "Integers for price and mileage (remove $ and commas). "
        "If price appears only in the TITLE like '$2,900', set price=2900. "
        "Ignore phone numbers and ZIP codes when deciding price. "
        "Transmission one of: Automatic, Manual, CVT, Other, Unknown. "
        "Use null when unknown. Output ONLY JSON."
    )
    schema = {
        "type":"object",
        "properties":{
            "post_id":{"type":"string"},
            "url":{"type":["string","null"]},
            "title":{"type":["string","null"]},
            "price":{"type":["integer","null"]},
            "year":{"type":["integer","null"]},
            "make":{"type":["string","null"]},
            "model":{"type":["string","null"]},
            "trim":{"type":["string","null"]},
            "mileage":{"type":["integer","null"]},
            "vin":{"type":["string","null"]},
            "color":{"type":["string","null"]},
            "transmission":{"type":["string","null"]},
            "condition":{"type":["string","null"]},
            "location":{"type":["string","null"]},
            "posted_iso":{"type":["string","null"]},
            "body":{"type":["string","null"]},
            "attrs_json":{"type":["object","null"]}
        },
        "required":["post_id"]
    }
    fewshot = (
        "EX1: 2016 Honda Civic LX - $9,900 - West Haven → "
        "price=9900, year=2016, make=Honda, model=Civic, trim=LX, location='West Haven'\n"
        "EX2: 2013 Hyundai Sonata GLS $3,900 Milford → "
        "price=3900, year=2013, make=Hyundai, model=Sonata, trim=GLS, location='Milford'\n"
    )
    prompt = (
        f"{system}\n\n{fewshot}\n"
        f"POST_ID: {post_id}\nURL: {url or ''}\n\nLISTING TEXT:\n{text}\n\nReturn ONLY JSON."
    )
    
    resp = _model().generate_content(
        [_model.Part.from_text(prompt)],
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": schema
        }
    )


    return json.loads(resp.text)

# -------------------- Deterministic TXT parsing --------------------

def _ensure_misc_list(attrs: dict):
    if not isinstance(attrs.get("misc"), list):
        attrs["misc"] = []
    return attrs["misc"]

def parse_txt(body: str) -> Dict:
    """
    Parses the scraper-made TXT format into a dict (best-effort).
    Fields: title, price, location, url, posted_iso, attrs_json, body, year/make/model heuristics.
    """
    lines = body.splitlines()
    data: Dict = {"attrs_json": {}}
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("Title:"):
            data["title"] = line.split(":", 1)[1].strip() or None

        elif line.startswith("Price:"):
            m = PRICE_LINE_RE.search(line)
            data["price"] = int(m.group(1).replace(",", "")) if m else None

        elif line.startswith("Neighborhood:"):
            data["location"] = line.split(":", 1)[1].strip() or None

        elif line.startswith("URL:"):
            data["url"] = line.split(":", 1)[1].strip() or None

        elif line.startswith("Posted:"):
            data["posted_iso"] = line.split(":", 1)[1].strip() or None

        elif line.startswith("Attributes:"):
            i += 1
            while i < len(lines) and lines[i].strip().startswith("-"):
                kv = lines[i].strip()[1:].strip()
                if ":" in kv:
                    k, v = kv.split(":", 1)
                    data["attrs_json"][k.strip().lower()] = v.strip()
                else:
                    misc = _ensure_misc_list(data["attrs_json"])
                    misc.append(kv)
                i += 1
            continue  # already advanced i

        elif line == "BODY:" or line.endswith("BODY:"):
            data["body"] = "\n".join(lines[i+1:]).strip()
            break

        i += 1

    # Year/make/model heuristics from title
    title = data.get("title") or ""
    ym = YEAR_RE.search(title)
    data["year"] = int(ym.group(0)) if ym else None
    t2 = YEAR_RE.sub("", title).strip()
    parts = re.split(r"\s+", t2) if t2 else []
    data["make"]  = parts[0].title() if parts else None
    data["model"] = " ".join(parts[1:3]).title() if len(parts) > 1 else None

    # Post ID from URL if available
    if data.get("url"):
        m = POST_ID_RE.search(data["url"])
        if m:
            data["post_id"] = m.group(1)

    return data

# -------------------- Misc helpers --------------------

def parse_url_from_text(text: str) -> Optional[str]:
    for line in text.splitlines():
        if line.strip().lower().startswith("url:"):
            return line.split(":", 1)[1].strip()
    return None

def post_id_from_any(name: str, text: str, url: Optional[str]) -> Optional[str]:
    for s in (name, url or "", text):
        m = FNAME_ID_RE.search(s)
        if m: return m.group(1)
        m2 = POST_ID_RE.search(s)
        if m2: return m2.group(1)
    return None

def price_fallback(title: str, body: str) -> Optional[int]:
    for s in (title or "", body or ""):
        m = PRICE_FALLBACK_RE.search(s)
        if not m:
            continue
        n = int(re.sub(r"[^\d]", "", m.group(1)))
        if 500 <= n <= 150_000:
            return n
    return None

def _list_run_ids() -> List[str]:
    # prefixes like 'craigslist/20251017T185944Z/'
    it = _sc().list_blobs(BUCKET, prefix=f"{PREFIX}/", delimiter="/")
    # IMPORTANT: consume iterator so .prefixes gets populated
    _ = list(it)
    run_ids = []
    for p in sorted(getattr(it, "prefixes", [])):
        # p == 'craigslist/<run_id>/' → grab <run_id>
        parts = p.strip("/").split("/")
        if len(parts) == 2 and parts[0] == PREFIX:
            run_ids.append(parts[1])
    return run_ids

def _list_txt_for_run(run_id: str) -> List[str]:
    pfx = f"{PREFIX}/{run_id}/txt/"
    return [b.name for b in _sc().list_blobs(BUCKET, prefix=pfx) if b.name.endswith(".txt")]

# -------------------- HTTP entry point --------------------

def extract_http(request: Request):
    logging.getLogger().setLevel(logging.INFO)

    if not (PROJECT_ID and BUCKET):
        return jsonify({"ok": False, "error": "missing PROJECT_ID or GCS_BUCKET"}), 500

    # Accept JSON body: {"run_id":"YYYYMMDDThhmmssZ", "max_files": 100, "overwrite": false}
    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    run_id = body.get("run_id")
    max_files = int(body.get("max_files") or 0)  # 0 = no limit
    overwrite = bool(body.get("overwrite") or False)

    # Pick newest run if none provided
    if not run_id:
        runs = _list_run_ids()
        if not runs:
            return jsonify({"ok": False, "error": "no runs found under prefix", "prefix": PREFIX}), 200
        run_id = runs[-1]

    # Gather candidates
    txt_names = _list_txt_for_run(run_id)
    processed_state = _read_state(run_id)

    # Optionally skip ones we've done already (unless overwrite=True)
    if not overwrite:
        txt_names = [n for n in txt_names if n not in processed_state]

    if max_files > 0:
        txt_names = txt_names[:max_files]

    processed = 0
    written = 0
    errors = 0

    # Init model once per request
    try:
        _model()
    except Exception as e:
        logging.error(f"[extractor-cron] Vertex init failed: {e}\n{traceback.format_exc()}")
        return jsonify({"ok": False, "error": "vertex_init_failed"}), 500

    for name in txt_names:
        try:
            text = _download_text(name)
            parsed = parse_txt(text)

            # derive url/post_id if missing
            url = parsed.get("url") or parse_url_from_text(text)
            post_id = parsed.get("post_id") or post_id_from_any(name, text, url)
            if not post_id:
                logging.warning(f"[extractor-cron] no post_id for {name}; skipping")
                processed_state.add(name)
                processed += 1
                continue

            # call model to enrich/clean
            raw = {}
            if USE_VERTEX:
                try:
                    raw = model_extract_json(text, url, post_id)
                except Exception as e:
                    logging.error(f"[extractor-cron] Vertex extract failed for {name}: {e}")
                    raw = {}


            # merge: prefer model values when present (not None), fallback to parsed
            merged = dict(parsed)
            for k, v in raw.items():
                if v is not None:
                    merged[k] = v

            merged["post_id"] = post_id
            merged["url"] = merged.get("url") or url

            # price fallback if still missing
            if merged.get("price") is None:
                pf = price_fallback(merged.get("title") or "", merged.get("body") or "")
                if pf is not None:
                    merged["price"] = pf

            # ensure attrs_json is an object with list-able misc
            if merged.get("attrs_json") is None:
                merged["attrs_json"] = {}
            _ensure_misc_list(merged["attrs_json"])

            # validate
            item = Listing(**merged)

            # write JSON (overwrite allowed)
            out_key = f"{PREFIX}/{run_id}/structured/json2/{post_id}.json"
            _upload_json(out_key, item.dict())
            written += 1

            # mark processed
            processed_state.add(name)
            processed += 1

        except Exception as e:
            errors += 1
            logging.error(f"[extractor-cron] failed {name}: {e}\n{traceback.format_exc()}")
            # Do not mark processed -> it will retry on next cron unless overwrite=True was used

    # Persist state
    try:
        _write_state(run_id, processed_state)
    except Exception as e:
        logging.error(f"[extractor-cron] failed to write state for run {run_id}: {e}")

    result = {
        "ok": True,
        "run_id": run_id,
        "processed": processed,
        "written_json": written,
        "errors": errors
    }
    logging.info(json.dumps(result))
    return jsonify(result), 200
