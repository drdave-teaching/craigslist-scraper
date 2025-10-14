# Cloud Functions (Gen2) HTTP entrypoint: scrape → save to GCS
# Runs your Craigslist scraper and writes:
#   - index CSV:   gs://<GCS_BUCKET>/<PREFIX>/<run_id>/index.csv
#   - one TXT per listing under: .../<run_id>/txt/<file>.txt
# Trigger with Cloud Scheduler (OIDC) every 6 hours.

import os, sys, re, time, json, io
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
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "craigslist")  # "folder" prefix

# Default query parameters for Craigslist search
QUERY_PARAMS = {
    "hasPic": os.getenv("CL_HAS_PIC", "1"),
    "min_auto_year": os.getenv("CL_MIN_YEAR", "2012"),
    "srchType": os.getenv("CL_SRCH_TYPE", "T"),  # search titles only
}

# Regex helpers
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
POST_ID_RE = re.compile(r"/(\d{8,12})\.html(?:\?.*)?$")

# ------------- HTTP helpers -------------
def build_url(start: int = 0) -> str:
    params = QUERY_PARAMS.copy()
    if start > 0:
        params["s"] = start
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_SITE}{SEARCH_PATH}?{qs}"

def fetch_html(url: str) -> Optional[str]:
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            print(f"[warn] {resp.status_code} for {url}", file=sys.stderr)
            return None
        return resp.text
    except requests.RequestException as e:
        print(f"[warn] request failed for {url}: {e}", file=sys.stderr)
        return None

# ------------- Parsers -------------
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
    if link and link.startswith("/"):
        link = BASE_SITE + link

    price_el = li.select_one("span.result-price") or li.select_one("span.price")
    price = None
    if price_el:
        try:
            price = int(re.sub(r"[^\d]", "", price_el.get_text()))
        except Exception:
            pass

    hood_el = li.select_one("span.result-hood")
    hood = hood_el.get_text(strip=True).strip("()") if hood_el else None

    year = None
    if title:
        m = YEAR_RE.search(title)
        if m:
            year = int(m.group(0))

    return {
        "title": title,
        "price": price,
        "year_in_title": year,
        "hood": hood,
        "url": link
    }

def parse_search_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items = soup.select("li.result-row") or soup.select("li.cl-static-search-result") or soup.select("li[class*=result]")
    return [parse_listing(li) for li in items]

def split_make_model(t: Optional[str]):
    if not isinstance(t, str):
        return (None, None)
    t2 = YEAR_RE.sub("", t).strip()
    parts = re.split(r"\s+", t2)
    if not parts:
        return (None, None)
    make = parts[0].title()
    model = " ".join(parts[1:3]).title() if len(parts) > 1 else None
    return (make, model)

def extract_post_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = POST_ID_RE.search(url)
    return m.group(1) if m else None

def parse_detail_page(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    body_el = (
        soup.select_one("#postingbody")
        or soup.select_one("section#postingbody")
        or soup.select_one("div[id*=postingbody]")
    )
    if body_el:
        body_text = body_el.get_text("\n", strip=True)
        body_text = re.sub(r"^\s*QR Code Link to This Post\s*\n?", "", body_text, flags=re.I)
    else:
        meta = soup.select_one("meta[name='description']")
        body_text = meta["content"].strip() if meta and meta.has_attr("content") else ""

    attrs = {}
    misc_vals = []
    for s in soup.select("p.attrgroup span"):
        txt = s.get_text(" ", strip=True)
        if ":" in txt:
            k, v = txt.split(":", 1)
            attrs[k.strip().lower()] = v.strip()
        else:
            misc_vals.append(txt)
    if misc_vals:
        attrs["misc"] = misc_vals

    when = None
    t_el = soup.select_one("time[datetime]")
    if t_el and t_el.has_attr("datetime"):
        when = t_el["datetime"]

    return {"body_text": body_text, "attrs": attrs, "posted": when}

# ------------- GCS helpers -------------
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

def gcs_blob_exists(bucket: str, path: str) -> bool:
    b = storage_client().bucket(bucket)
    return b.blob(path).exists()


# ------------- Core scrape -------------
def scrape(max_pages: int) -> pd.DataFrame:
    all_rows = []
    for page in range(max_pages):
        start = page * RESULTS_PER_PAGE
        url = build_url(start=start)
        print(f"[info] fetching page {page+1} → {url}")
        html = fetch_html(url)
        if not html:
            break
        rows = parse_search_page(html)
        if not rows:
            print("[info] no rows found; stopping.")
            break
        all_rows.extend(rows)
        time.sleep(REQUEST_DELAY_SECS)
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if not df.empty:
        df[["make_guess", "model_guess"]] = df["title"].apply(
            lambda t: pd.Series(split_make_model(t))
        )
    return df

def save_listing_txt(row: Dict, detail: Dict) -> str:
    pid = extract_post_id(row.get("url")) or ""
    parts = [
        str(row.get("year_in_title") or ""),
        row.get("make_guess") or "",
        row.get("model_guess") or "",
        pid
    ]
    base = "-".join([p for p in parts if p]).strip("-") or (row.get("title") or "listing")
    safe = re.sub(r"[^\w.\-]+", "_", base)[:140].strip("_") or "listing"
    # compose object path under run_id/txt/...
    name = f"{safe}.txt"
    return name

# ------------- HTTP ENTRYPOINT -------------
def scrape_http(request: Request):
    """
    HTTP-triggered entrypoint (Cloud Functions Gen2).
    Optional JSON body:
      {"max_pages": 2, "prefix": "craigslist/ct"}
    """
    if not GCS_BUCKET:
        return jsonify({"error": "GCS_BUCKET env var required"}), 500

    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    max_pages = int(body.get("max_pages", MAX_PAGES_DEFAULT))
    prefix = body.get("prefix", OUTPUT_PREFIX)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_prefix = PurePosixPath(prefix) / run_id

    # scrape index
    df = scrape(max_pages=max_pages)

    # upload CSV index
    csv_key = str(base_prefix / "index.csv")
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    csv_uri = gcs_upload_text(GCS_BUCKET, csv_key, csv_buf.getvalue(), content_type="text/csv")

    # detail pages → one TXT per listing
    saved, skipped = 0, 0
    txt_prefix = base_prefix / "txt"
    for _, row in df.iterrows():
        url = row.get("url")
        if not url:
            skipped += 1
            continue
        html = fetch_html(url)
        if not html:
            skipped += 1
            continue
        detail = parse_detail_page(html)

        # assemble the .txt content
        lines = []
        lines.append(f"Title: {row.get('title') or ''}")
        price = row.get("price")
        lines.append(f"Price: ${price:,}" if price is not None else "Price: ")
        lines.append(f"Neighborhood: {row.get('hood') or ''}")
        lines.append(f"URL: {row.get('url') or ''}")
        lines.append(f"Posted: {detail.get('posted') or ''}")

        attrs = detail.get("attrs") or {}
        if attrs:
            lines.append("Attributes:")
            for k, v in attrs.items():
                if isinstance(v, list):
                    v = ", ".join(v)
                lines.append(f"  - {k}: {v}")

        lines.append("-" * 40)
        lines.append("BODY:")
        lines.append(detail.get("body_text") or "")
        text = "\n".join(lines)

        obj_name = str(txt_prefix / save_listing_txt(row.to_dict(), detail))
        if gcs_blob_exists(GCS_BUCKET, obj_name):
            skipped += 1
            continue
        
        gcs_upload_text(GCS_BUCKET, obj_name, text, content_type="text/plain")
        saved += 1

        time.sleep(DETAIL_REQUEST_DELAY_SECS)

    result = {
        "run_id": run_id,
        "bucket": GCS_BUCKET,
        "index_csv": csv_uri,
        "txt_saved": saved,
        "txt_skipped": skipped,
        "rows": int(len(df)),
    }
    return jsonify(result), 200
