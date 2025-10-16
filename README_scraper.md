This scraper code is very good - but there are two ways to deploy. One is locally from GCP and the other is from your GitHub repo.

# Path A: Running from GCP
## 0) Repo layout (students)
You can do this in VSCode or directly on GitHub.

```
craigslist-scraper/
├─ README.md
├─ main.py                 # Cloud Function (Gen2) version of the scraper
├─ requirements.txt
└─ .github/
   └─ workflows/
      └─ deploy.yml        # optional CI; manual gcloud is fine too
```

## 1) `main.py` (Cloud Functions Gen2 scraper → writes to GCS)

Runs the search, de-dupes by URL within a run, saves index.csv and one .txt per listing into a timestamped folder in Cloud Storage.
```python
# Cloud Functions (Gen2) HTTP entrypoint: scrape → save to GCS
# Output:
#   gs://<GCS_BUCKET>/<OUTPUT_PREFIX>/<run_id>/index.csv
#   gs://<GCS_BUCKET>/<OUTPUT_PREFIX>/<run_id>/txt/<file>.txt

import os, sys, re, time, io
from datetime import datetime, timezone
from typing import List, Dict, Optional
from pathlib import PurePosixPath

import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import storage
from flask import Request, jsonify

# ---------------- CONFIG (env-first) ----------------
BASE_SITE = os.getenv("BASE_SITE", "https://newhaven.craigslist.org")
SEARCH_PATH = os.getenv("SEARCH_PATH", "/search/cta")
RESULTS_PER_PAGE = int(os.getenv("RESULTS_PER_PAGE", "120"))
MAX_PAGES_DEFAULT = int(os.getenv("MAX_PAGES", "3"))
REQUEST_DELAY_SECS = float(os.getenv("REQUEST_DELAY_SECS", "1.0"))
DETAIL_REQUEST_DELAY_SECS = float(os.getenv("DETAIL_REQUEST_DELAY_SECS", "1.0"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "UConn-OPIM-Student-Scraper/1.0 (educational use; contact instructor)"
)

# Where to store in GCS
GCS_BUCKET = os.getenv("GCS_BUCKET")                   # REQUIRED
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "craigslist")

QUERY_PARAMS = { "hasPic": "1", "min_auto_year": "2012", "srchType": "T" }

YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
POST_ID_RE = re.compile(r"/(\d{8,12})\.html(?:\?.*)?$")

# ---------- HTTP ----------
def build_url(start: int = 0) -> str:
    params = QUERY_PARAMS.copy()
    if start > 0: params["s"] = start
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_SITE}{SEARCH_PATH}?{qs}"

def fetch_html(url: str) -> Optional[str]:
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            print(f"[warn] {r.status_code} for {url}", file=sys.stderr)
            return None
        return r.text
    except requests.RequestException as e:
        print(f"[warn] request failed for {url}: {e}", file=sys.stderr)
        return None

# ---------- Parse helpers ----------
def parse_listing(li) -> Dict:
    title_el = (
        li.select_one("a.result-title")
        or li.select_one("a.posting-title")
        or li.select_one("a[href*='/cto/']")
        or li.select_one("a[href*='/ctd/']")
        or li.select_one("a[href*='/cto?']")
    )
    title = title_el.get_text(strip=True) if title_el else None
    link = title_el["href"] if title_el and title_el.has_attr("href") else None
    if link and link.startswith("/"): link = BASE_SITE + link

    price_el = li.select_one("span.result-price") or li.select_one("span.price")
    price = None
    if price_el:
        try: price = int(re.sub(r"[^\d]", "", price_el.get_text()))
        except: pass

    hood_el = li.select_one("span.result-hood")
    hood = hood_el.get_text(strip=True).strip("()") if hood_el else None

    year = None
    if title:
        m = YEAR_RE.search(title)
        if m: year = int(m.group(0))

    return {"title": title, "price": price, "year_in_title": year, "hood": hood, "url": link}

def parse_search_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items = soup.select("li.result-row") or soup.select("li.cl-static-search-result") or soup.select("li[class*=result]")
    return [parse_listing(li) for li in items]

def split_make_model(t: Optional[str]):
    if not isinstance(t, str): return (None, None)
    t2 = YEAR_RE.sub("", t).strip()
    parts = re.split(r"\s+", t2)
    if not parts: return (None, None)
    make = parts[0].title()
    model = " ".join(parts[1:3]).title() if len(parts) > 1 else None
    return (make, model)

def extract_post_id(url: Optional[str]) -> Optional[str]:
    if not url: return None
    m = POST_ID_RE.search(url)
    return m.group(1) if m else None

def parse_detail_page(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    body_el = soup.select_one("#postingbody") or soup.select_one("section#postingbody") or soup.select_one("div[id*=postingbody]")
    if body_el:
        body_text = body_el.get_text("\n", strip=True)
        body_text = re.sub(r"^\s*QR Code Link to This Post\s*\n?", "", body_text, flags=re.I)
    else:
        meta = soup.select_one("meta[name='description']")
        body_text = meta["content"].strip() if meta and meta.has_attr("content") else ""

    attrs, misc_vals = {}, []
    for s in soup.select("p.attrgroup span"):
        txt = s.get_text(" ", strip=True)
        if ":" in txt:
            k, v = txt.split(":", 1)
            attrs[k.strip().lower()] = v.strip()
        else:
            misc_vals.append(txt)
    if misc_vals: attrs["misc"] = misc_vals

    when = None
    t_el = soup.select_one("time[datetime]")
    if t_el and t_el.has_attr("datetime"): when = t_el["datetime"]

    return {"body_text": body_text, "attrs": attrs, "posted": when}

# ---------- GCS ----------
_storage_client = None
def storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client

def gcs_upload_text(bucket: str, path: str, text: str, content_type: str = "text/plain"):
    b = storage_client().bucket(bucket)
    blob = b.blob(path)
    blob.upload_from_string(text, content_type=content_type)
    return f"gs://{bucket}/{path}"

# ---------- Core scrape ----------
def scrape(max_pages: int) -> pd.DataFrame:
    rows = []
    for page in range(max_pages):
        start = page * RESULTS_PER_PAGE
        url = build_url(start=start)
        print(f"[info] fetching page {page+1} → {url}")
        html = fetch_html(url)
        if not html: break
        items = parse_search_page(html)
        if not items:
            print("[info] no rows found; stopping.")
            break
        rows.extend(items)
        time.sleep(REQUEST_DELAY_SECS)
    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if not df.empty:
        df[["make_guess","model_guess"]] = df["title"].apply(lambda t: pd.Series(split_make_model(t)))
    return df

def save_listing_txt(row: Dict, detail: Dict) -> str:
    pid = extract_post_id(row.get("url")) or ""
    parts = [str(row.get("year_in_title") or ""), row.get("make_guess") or "", row.get("model_guess") or "", pid]
    base = "-".join([p for p in parts if p]).strip("-") or (row.get("title") or "listing")
    safe = re.sub(r"[^\w.\-]+", "_", base)[:140].strip("_") or "listing"
    return f"{safe}.txt"

# ---------- HTTP entrypoint ----------
def scrape_http(request: Request):
    if not GCS_BUCKET:
        return jsonify({"error": "GCS_BUCKET env var required"}), 500

    body = request.get_json(silent=True) or {}
    max_pages = int(body.get("max_pages", MAX_PAGES_DEFAULT))
    prefix = body.get("prefix", OUTPUT_PREFIX)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_prefix = PurePosixPath(prefix) / run_id

    df = scrape(max_pages=max_pages)

    # index.csv
    csv_key = str(base_prefix / "index.csv")
    buf = io.StringIO(); df.to_csv(buf, index=False)
    csv_uri = gcs_upload_text(GCS_BUCKET, csv_key, buf.getvalue(), "text/csv")

    # details → txt/
    saved, skipped = 0, 0
    txt_prefix = base_prefix / "txt"
    for _, row in df.iterrows():
        url = row.get("url")
        if not url:
            skipped += 1; continue
        html = fetch_html(url)
        if not html:
            skipped += 1; continue
        detail = parse_detail_page(html)

        lines = [
            f"Title: {row.get('title') or ''}",
            f"Price: ${row['price']:,}" if row.get("price") is not None else "Price: ",
            f"Neighborhood: {row.get('hood') or ''}",
            f"URL: {row.get('url') or ''}",
            f"Posted: {detail.get('posted') or ''}",
        ]
        attrs = detail.get("attrs") or {}
        if attrs:
            lines.append("Attributes:")
            for k, v in attrs.items():
                if isinstance(v, list): v = ", ".join(v)
                lines.append(f"  - {k}: {v}")
        lines += ["-"*40, "BODY:", detail.get("body_text") or ""]
        text = "\n".join(lines)

        obj_name = str(txt_prefix / save_listing_txt(row.to_dict(), detail))
        gcs_upload_text(GCS_BUCKET, obj_name, text, "text/plain")
        saved += 1
        time.sleep(DETAIL_REQUEST_DELAY_SECS)

    return jsonify({
        "run_id": run_id,
        "bucket": GCS_BUCKET,
        "index_csv": csv_uri,
        "txt_saved": saved,
        "txt_skipped": skipped,
        "rows": int(len(df)),
    }), 200
```

## 2) `requirements.txt`
```python
functions-framework==3.*
google-cloud-storage>=2.16.0
beautifulsoup4>=4.12.2
lxml>=5.2.1
pandas>=2.1.0
requests>=2.31.0
```

## 3) One-time GCP setup (students run in Cloud Shell)
```python
# set project/region
PROJECT_ID="craigslist-scraper-v2"
REGION="us-central1"
gcloud config set project $PROJECT_ID

# enable services
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com \
  artifactregistry.googleapis.com cloudscheduler.googleapis.com

# service accounts
gcloud iam service-accounts create sa-scraper   --display-name="Craigslist runtime SA"
gcloud iam service-accounts create sa-scheduler --display-name="Scheduler invoker SA"

RUNTIME_SA="sa-scraper@$PROJECT_ID.iam.gserviceaccount.com"
SCHEDULER_SA="sa-scheduler@$PROJECT_ID.iam.gserviceaccount.com"

# bucket for outputs
BUCKET="craigslist-data-$PROJECT_ID"
gsutil mb -l $REGION gs://$BUCKET
gsutil iam ch serviceAccount:$RUNTIME_SA:roles/storage.objectAdmin gs://$BUCKET

# (Gen2 first-deploy IAM)
PROJECT_NUMBER="$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')"
CLOUD_BUILD_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"
CLOUD_RUN_SA="service-${PROJECT_NUMBER}@serverless-robot-prod.iam.gserviceaccount.com"
CLOUD_FUNCTIONS_SA="service-${PROJECT_NUMBER}@gcf-admin-robot.iam.gserviceaccount.com"
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" --member="serviceAccount:$CLOUD_BUILD_SA" --role="roles/iam.serviceAccountUser"
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" --member="serviceAccount:$CLOUD_RUN_SA"   --role="roles/iam.serviceAccountUser"
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" --member="serviceAccount:$CLOUD_FUNCTIONS_SA" --role="roles/iam.serviceAccountUser"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$CLOUD_BUILD_SA" --role="roles/artifactregistry.writer"
```

## 4) Deploy the function (from the repo folder)
```python
git clone https://github.com/drdave-teaching/craigslist-scraper.git
cd craigslist-scraper

FUNCTION="craigslist-scraper"

gcloud functions deploy "$FUNCTION" \
  --gen2 \
  --region="$REGION" \
  --runtime=python311 \
  --entry-point=scrape_http \
  --trigger-http \
  --service-account="$RUNTIME_SA" \
  --no-allow-unauthenticated \
  --timeout=540s \
  --memory=1Gi \
  --source=. \
  --set-env-vars="GCS_BUCKET=$BUCKET,OUTPUT_PREFIX=craigslist,BASE_SITE=https://newhaven.craigslist.org,SEARCH_PATH=/search/cta,MAX_PAGES=3,REQUEST_DELAY_SECS=1.0,DETAIL_REQUEST_DELAY_SECS=1.0"
```

## Get the private URL
```python
FUNC_URL="$(gcloud functions describe $FUNCTION --region=$REGION --format='value(serviceConfig.uri)')"
echo $FUNC_URL
```

## Allow the scheduler to invoke
```python
gcloud run services add-iam-policy-binding "$FUNCTION" \
  --region="$REGION" \
  --member="serviceAccount:$SCHEDULER_SA" \
  --role="roles/run.invoker"
```

## 5) Schedule every 6 hours (Cloud Scheduler, OIDC)
```python
JOB_ID="craigslist-scraper-6h"

gcloud scheduler jobs create http "$JOB_ID" \
  --location="$REGION" \
  --schedule="0 */6 * * *" \
  --time-zone="America/New_York" \
  --http-method=POST \
  --uri="$FUNC_URL" \
  --oidc-service-account-email="$SCHEDULER_SA" \
  --oidc-token-audience="$FUNC_URL" \
  --headers="Content-Type=application/json" \
  --message-body='{"max_pages": 3, "prefix": "craigslist"}' \
|| gcloud scheduler jobs update http "$JOB_ID" \
  --location="$REGION" \
  --schedule="0 */6 * * *" \
  --time-zone="America/New_York" \
  --http-method=POST \
  --uri="$FUNC_URL" \
  --oidc-service-account-email="$SCHEDULER_SA" \
  --oidc-token-audience="$FUNC_URL" \
  --headers="Content-Type=application/json" \
  --message-body='{"max_pages": 3, "prefix": "craigslist"}'
```

## Manual test
```python
gcloud scheduler jobs run "$JOB_ID" --location="$REGION"
# or direct:
curl -X POST "$FUNC_URL" -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     -H "Content-Type: application/json" -d '{"max_pages":2,"prefix":"craigslist/test"}'
```

## Expected bucket structure
```
gs://craigslist-data-craigslist-scraper-v2/craigslist/<YYYYMMDDTHHMMSSZ>/
  ├─ index.csv
  └─ txt/*.txt
```

## 6) (Optional) “Don’t re-save old listings” tweak
Add this helper and check before writing each TXT (you can find these in Dave's repo under `main.py`):
```python
# near GCS helpers
def gcs_blob_exists(bucket: str, path: str) -> bool:
    b = storage_client().bucket(bucket)
    return b.blob(path).exists()

# in the detail loop, before gcs_upload_text(...)
obj_name = str(txt_prefix / save_listing_txt(row.to_dict(), detail))
if gcs_blob_exists(GCS_BUCKET, obj_name):
    skipped += 1
    continue
```
