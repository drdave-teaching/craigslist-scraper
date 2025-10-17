# Event-driven extractor for new TXT files written by the scraper.
# Trigger: GCS object finalized (any object); we early-exit unless it’s .../txt/*.txt
# Output: one JSON per listing at .../structured/json/<post_id>.json

import os, re, json, logging
from typing import Optional
from google.cloud import storage
from vertexai.preview.generative_models import GenerativeModel, Part
import vertexai
from pydantic import BaseModel, Field, validator
from google.api_core import retry as gax_retry

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION   = os.getenv("LOCATION", "us-central1")
BUCKET     = os.getenv("GCS_BUCKET")  # same as your scraper bucket

POST_ID_RE = re.compile(r"/([0-9]{8,12})\.html")
FNAME_ID_RE = re.compile(r"([0-9]{8,12})")

# ---------- Pydantic schema ----------
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
    def clean_vin(cls, v):
        if not v: return v
        return v.upper().replace(" ", "").replace("-", "")

# ---------- helpers ----------
PRICE_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:[, ]?[0-9]{3})+|[0-9]{3,6})\b")

# if numeric for price doesn't exist...
def price_fallback(title: str, body: str) -> Optional[int]:
    # Search title first (most reliable), then body
    for s in (title or "", body or ""):
        m = PRICE_RE.search(s)
        if not m:
            continue
        n = int(re.sub(r"[^\d]", "", m.group(1)))
        # sanity range for used cars; tweak if you like
        if 500 <= n <= 150000:
            return n
    return None
    
# a sensible retry for transient HTTPS errors
import traceback

# Global singleton client (avoid reconnect cost per invocation)
_STORAGE = storage.Client()
_SC = None

def _sc() -> storage.Client:
    global _SC
    if _SC is None:
        _SC = storage.Client()
    return _SC

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

def read_text(bucket: str, key: str) -> str:
    # download_as_bytes + decode is more robust than download_as_text
    blob = _sc().bucket(bucket).blob(key)
    bs = blob.download_as_bytes(retry=READ_RETRY, timeout=120)
    return bs.decode("utf-8", errors="replace")

def write_json(bucket: str, key: str, data: dict):
    blob = _sc().bucket(bucket).blob(key)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json",
    )


def init_vertex() -> GenerativeModel:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    return GenerativeModel("gemini-1.5-pro")

def parse_url_from_text(text: str) -> Optional[str]:
    for line in text.splitlines():
        if line.strip().lower().startswith("url:"):
            return line.split(":", 1)[1].strip()
    return None

def post_id_from_any(name: str, text: str, url: Optional[str]) -> Optional[str]:
    for s in [name, url or "", text]:
        m = FNAME_ID_RE.search(s)
        if m: return m.group(1)
        m2 = POST_ID_RE.search(s)
        if m2: return m2.group(1)
    return None

def model_extract_json(model: GenerativeModel, text: str, url: Optional[str], post_id: str) -> dict:
    # Clear, test-like rules the model must follow
    system = (
        "You extract car-listing data as STRICT JSON that matches the provided schema. "
        "Return integers for price/mileage (remove $ and commas.)"
        "If price only appears in title like '$2,900', still set price=2900."
        "Ignore phone numbers and ZIP codes; prefer a single plausible car-sale price."
        "Use null when unknown."
        "If a field is unknown, use null. Follow these rules:\n"
        "1) price: integer USD with no symbols or commas; prefer an explicit 'Price:' line; "
        "   otherwise use a $#### pattern in the text. Example: '$3,900' -> 3900.\n"
        "2) year: 4-digit vehicle year (1950–2100), prefer the listing title, else attributes/body.\n"
        "3) make: manufacturer (e.g., Honda, Hyundai, Toyota). Only the brand name.\n"
        "4) model: vehicle family ONLY (e.g., Civic, Sonata, Camry). "
        "   DO NOT include trim (e.g., LX, SE), currency symbols, location names, mileage, or the year.\n"
        "5) trim: optional submodel/grade (e.g., LX, SE, Limited). Put trims here, not in model.\n"
        "6) location: prefer the 'Neighborhood' / hood field if present; otherwise a clear location token from text.\n"
        "7) posted_iso: if a timestamp is present, keep ISO-8601 (UTC or with offset).\n"
        "8) body: include the free-text description (or best available body text).\n"
        "9) attrs_json: any bullet attributes as an object; if none, use {} (empty object), not a string.\n"
        "Return ONLY JSON — no prose."
        "Don't hallucinate!"
    )

    schema = {
        "type": "object",
        "properties": {
            "post_id": {"type": "string"},
            "url": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
            "price": {"type": ["integer", "null"]},
            "year": {"type": ["integer", "null"]},
            "make": {"type": ["string", "null"]},
            "model": {"type": ["string", "null"]},
            "trim": {"type": ["string", "null"]},
            "mileage": {"type": ["integer", "null"]},
            "vin": {"type": ["string", "null"]},
            "color": {"type": ["string", "null"]},
            "transmission": {"type": ["string", "null"]},
            "condition": {"type": ["string", "null"]},
            "location": {"type": ["string", "null"]},
            "posted_iso": {"type": ["string", "null"]},
            "body": {"type": ["string", "null"]},
            "attrs_json": {"type": ["object", "null"]}
        },
        "required": ["post_id"]
    }

    # Two tiny few-shot hints to steer parsing of price/model
    fewshot = (
        "EXAMPLE 1 TITLE: 2016 Honda Civic LX - $9,900 - West Haven\n"
        "→ price=9900, year=2016, make=Honda, model=Civic, trim=LX, location=West Haven\n"
        "EXAMPLE 2 TITLE: 2013 Hyundai Sonata GLS $3,900 Milford\n"
        "→ price=3900, year=2013, make=Hyundai, model=Sonata, trim=GLS, location=Milford\n"
    )

    prompt = (
        f"{system}\n\n"
        f"{fewshot}\n"
        f"POST_ID: {post_id}\n"
        f"URL: {url or ''}\n\n"
        "LISTING TEXT:\n"
        f"{text}\n\n"
        "Return ONLY JSON that conforms to the schema."
    )

    resp = model.generate_content(
        [Part.from_text(prompt)],
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": schema
        }
    )
    return json.loads(resp.text)


def write_json(bucket: str, key: str, data: dict):
    client = storage.Client()
    b = client.bucket(bucket)
    bl = b.blob(key)
    bl.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")

def read_text(bucket: str, key: str) -> str:
    client = storage.Client()
    return client.bucket(bucket).blob(key).download_as_text()

# ---------- CloudEvent entrypoint ----------
from typing import Any, Tuple

def _get_bucket_name_from_event(event: Any, context: Any) -> Tuple[str, str]:
    """
    Supports BOTH formats:
    - Background (legacy): etl_gcs(data: dict, context)
      where data = {"bucket": "...", "name": "..."}
    - CloudEvent (Eventarc/Gen2): etl_gcs(event)
      where event.data = {"bucket": "...", "name": "..."}
    """
    # background (2 args)
    if context is not None and isinstance(event, dict):
        return event.get("bucket"), event.get("name")

    # cloudevent (1 arg)
    ce = getattr(event, "data", None) or event
    return (ce or {}).get("bucket"), (ce or {}).get("name")

def etl_gcs(event, context=None):
    bucket, name = _get_bucket_name_from_event(event, context)

    # Only handle TXT files the scraper created
    if not name or not name.endswith(".txt") or "/txt/" not in name:
        return "ignored", 204

    try:
        # Pull text, derive url + post_id
        text = read_text(bucket, name)
        url = parse_url_from_text(text)
        post_id = post_id_from_any(name, text, url)
        if not post_id:
            logging.warning(f"no post_id for {name}; skipping")
            return "no post_id", 204

        # Run model → JSON, then validate/normalize via Pydantic
        model = init_vertex()
        raw = model_extract_json(model, text, url, post_id)
        raw["post_id"] = post_id               # ensure set
        raw["url"] = raw.get("url") or url     # prefer model, fallback to parsed
        # prefer model, but backfill if missing...
        if raw.get("price") is None:
            p = price_fallback(raw.get("title") or "", raw.get("body") or "")
            if p is not None:
                raw["price"] = p
        item = Listing(**raw)                  # validate

        # Write JSON beside the run folder
        parts = name.split("/")                 # craigslist/<run_id>/txt/<file>.txt
        run_prefix = "/".join(parts[:2])        # craigslist/<run_id>
        out_key = f"{run_prefix}/structured/json/{post_id}.json"
        write_json(BUCKET, out_key, item.dict())
        logging.info(f"[extractor] wrote gs://{BUCKET}/{out_key}")
        return f"wrote gs://{BUCKET}/{out_key}", 200

    except Exception as e:
        import traceback
        logging.error(f"[extractor] failed {name}: {e}\n{traceback.format_exc()}")
        return "error", 500
