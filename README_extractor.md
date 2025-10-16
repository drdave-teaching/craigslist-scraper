# 1) Files to add to your repo (or Cloud Shell folder)
## Add these files to main branch `extractor_main.py`

```python
# Event-driven extractor for new TXT files written by the scraper.
# Trigger: GCS object finalized (any object); we early-exit unless it’s .../txt/*.txt
# Output: one JSON per listing at .../structured/json/<post_id>.json

import os, re, json, logging
from typing import Optional
from google.cloud import storage
from vertexai.preview.generative_models import GenerativeModel, Part
import vertexai
from pydantic import BaseModel, Field, validator

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
    system = (
        "Extract car listing data as strict JSON only. "
        "If unknown, use null. Price/mileage are integers. "
        "Transmission one of: Automatic, Manual, CVT, Other, Unknown."
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
    prompt = (
        f"{system}\n\nPOST_ID: {post_id}\nURL: {url or ''}\n\n"
        "LISTING TEXT:\n" + text + "\n\nReturn ONLY JSON."
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
def etl_gcs(event):
    # event.data holds { bucket, name, ... }
    data = event.data if hasattr(event, "data") else event
    bucket = data.get("bucket")
    name   = data.get("name")  # object path

    # Only handle TXT files the scraper created
    if not name or not name.endswith(".txt") or "/txt/" not in name:
        return "ignored", 204

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

    item = Listing(**raw)  # raises if badly formed

    # Write one JSON per listing
    # Derive run_id = first 2 path parts after 'craigslist/', e.g. craigslist/<run_id>/txt/...
    parts = name.split("/")
    # name looks like: craigslist/<run_id>/txt/<file>.txt
    run_prefix = "/".join(parts[:2])  # craigslist/<run_id>
    out_key = f"{run_prefix}/structured/json/{post_id}.json"
    write_json(BUCKET, out_key, item.dict())
    return f"wrote gs://{BUCKET}/{out_key}", 200
```
## `extractor_requirements.txt`

```python
functions-framework==3.*
google-cloud-storage>=2.16.0
google-cloud-aiplatform>=1.56.0
pydantic>=2.6.0
```

# 2) One-time GCP setup for the extractor
Run these in Cloud Shell (slow and steady):

```python
# --- variables ---
PROJECT_ID="craigslist-scraper-v2"
REGION="us-central1"
BUCKET="craigslist-data-craigslist-scraper-v2"

# service account for the extractor
gcloud iam service-accounts create sa-extractor \
  --display-name="CL Extractor SA"

EXTRACTOR_SA="sa-extractor@$PROJECT_ID.iam.gserviceaccount.com"

# enable needed services
gcloud services enable \
  aiplatform.googleapis.com \
  eventarc.googleapis.com \
  cloudfunctions.googleapis.com \
  run.googleapis.com \
  pubsub.googleapis.com

# permissions: read/write bucket + Vertex AI
gsutil iam ch serviceAccount:$EXTRACTOR_SA:roles/storage.objectAdmin gs://$BUCKET
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$EXTRACTOR_SA" \
  --role="roles/aiplatform.user"
```

# 3) Deploy the event-driven Cloud Function (Gen 2)
From the folder that has `extractor_main.py` and `extractor_requirements.txt`:

```python
# if you cloned your repo in Cloud Shell:
cd ~/craigslist-scraper

# deploy the extractor; event trigger = GCS object finalized on your bucket
gcloud functions deploy craigslist-extractor \
  --gen2 \
  --region="$REGION" \
  --runtime=python311 \
  --entry-point=etl_gcs \
  --service-account="$EXTRACTOR_SA" \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=$BUCKET" \
  --timeout=540s \
  --memory=1Gi \
  --source=. \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,LOCATION=$REGION,GCS_BUCKET=$BUCKET" \
  --requirements extractor_requirements.txt
```

# 4) Test it
Pick one of your recent .txt paths and re-upload it to fire the trigger.

```python
# list a recent txt file
gsutil ls gs://$BUCKET/craigslist/*/txt/*.txt | tail -n 1

# copy it to a new name (re-uploads and triggers)
LATEST_TXT="$(gsutil ls gs://$BUCKET/craigslist/*/txt/*.txt | tail -n 1)"
RUN_PREFIX="$(dirname "$LATEST_TXT")"
gsutil cp "$LATEST_TXT" "${RUN_PREFIX}/test-reupload.txt"
```

Now you should see a new JSON here:
```
gs://craigslist-data-craigslist-scraper-v2/craigslist/<run_id>/structured/json/<post_id>.json
```

# 5) How it integrates with your current pipeline
* Your existing scraper still runs every 6 hours and writes index.csv + txt/.
* Each new .txt automatically triggers the extractor and produces a JSON alongside it.
* Later, we’ll create a BigQuery external table on .../structured/json/*.json. (No need to process historical runs unless you want to—just re-copy old .txt to re-emit JSON.)
