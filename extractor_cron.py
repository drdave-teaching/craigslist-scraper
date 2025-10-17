# HTTP cron extractor for GCF Gen2. No Eventarc.
# Scans GCS for new craigslist/*/txt/*.txt, writes JSON to craigslist/*/structured/json/*.json
# Tracks processed TXT keys in craigslist/state/last_extracted.txt

import os, io, json, re, logging, traceback
from typing import Dict, List, Set
from google.cloud import storage
from flask import Request, jsonify

BUCKET = os.getenv("GCS_BUCKET")
PREFIX = os.getenv("OUTPUT_PREFIX", "craigslist")
STATE_KEY = f"{PREFIX}/state/last_extracted.txt"   # stores processed blob names

POST_ID_RE = re.compile(r"/(\d{8,12})\.html")
PRICE_RE   = re.compile(r"\$?([\d,]+)")
YEAR_RE    = re.compile(r"\b(19|20)\d{2}\b")

def _sc():
    return storage.Client()

def _read_text(bucket: str, key: str) -> str:
    return _sc().bucket(bucket).blob(key).download_as_text()

def _write_json(bucket: str, key: str, data: dict):
    _sc().bucket(bucket).blob(key).upload_from_string(
        json.dumps(data, ensure_ascii=False), content_type="application/json"
    )

def _read_state(bucket: str, key: str) -> Set[str]:
    b = _sc().bucket(bucket).blob(key)
    if not b.exists():
        return set()
    try:
        return set(ln.strip() for ln in b.download_as_text().splitlines() if ln.strip())
    except Exception:
        # corrupt/empty → reset
        return set()

def _write_state(bucket: str, key: str, processed: Set[str]):
    text = "\n".join(sorted(processed))
    _sc().bucket(bucket).blob(key).upload_from_string(text, content_type="text/plain")

def _ensure_misc_list(attrs: dict):
    """
    Ensure attrs['misc'] is always a list before appending.
    Handles cases where a prior run accidentally stored a string.
    """
    if not isinstance(attrs.get("misc"), list):
        attrs["misc"] = []
    return attrs["misc"]

def parse_txt(body: str) -> Dict:
    """
    Expected lines from scraper:
      Title: ...
      Price: $12345
      Neighborhood: ...
      URL: https://.../1234567890.html
      Posted: 2025-01-01T12:34:56Z
      Attributes:
        - odometer: 123,456
        - (sometimes bare flags like "clean title")
      ----------------------------------------
      BODY:
      free text...
    """
    lines = body.splitlines()
    data = {"attrs_json": {}}
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("Title:"):
            data["title"] = line.split(":", 1)[1].strip() or None

        elif line.startswith("Price:"):
            m = PRICE_RE.search(line)
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
                kv = lines[i].strip()[1:].strip()  # drop leading "- "
                if ":" in kv:
                    k, v = kv.split(":", 1)
                    data["attrs_json"][k.strip().lower()] = v.strip()
                else:
                    # bare flags like "clean title"
                    misc = _ensure_misc_list(data["attrs_json"])
                    misc.append(kv)
                i += 1
            continue  # we've already advanced i inside this block

        elif line == "BODY:" or line.endswith("BODY:"):
            # everything after this is free text
            data["body"] = "\n".join(lines[i+1:]).strip()
            break

        i += 1

    # Heuristics: year/make/model from title
    title = data.get("title") or ""
    ym = YEAR_RE.search(title)
    data["year"] = int(ym.group(0)) if ym else None
    title_wo_year = YEAR_RE.sub("", title).strip()
    parts = title_wo_year.split()
    data["make"]  = parts[0].title() if parts else None
    data["model"] = " ".join(parts[1:3]).title() if len(parts) > 1 else None

    # Post ID from URL if possible
    if data.get("url"):
        m = POST_ID_RE.search(data["url"])
        if m:
            data["post_id"] = m.group(1)

    return data

def extract_http(request: Request):
    if not BUCKET:
        return jsonify({"error": "missing GCS_BUCKET"}), 500

    # load state of processed TXT object names
    done = _read_state(BUCKET, STATE_KEY)

    # find new txt files
    sc = _sc()
    to_process: List[str] = []
    for blob in sc.list_blobs(BUCKET, prefix=f"{PREFIX}/"):
        n = blob.name
        if n.endswith(".txt") and "/txt/" in n and n not in done:
            to_process.append(n)

    processed_now: List[str] = []
    written = 0
    errors = 0

    for name in to_process:
        try:
            text = _read_text(BUCKET, name)
            item = parse_txt(text)

            # derive post_id if missing (fallback to digits in filename)
            if not item.get("post_id"):
                m = re.search(r"(\d{8,12})", name)
                if m:
                    item["post_id"] = m.group(1)

            if not item.get("post_id"):
                logging.warning(f"[extractor] no post_id for {name}; skipping")
                done.add(name)
                processed_now.append(name)
                continue

            # output path: craigslist/<run_id>/structured/json/<post_id>.json
            parts = name.split("/")
            run_prefix = "/".join(parts[:2])   # craigslist/<run_id>
            out_key = f"{run_prefix}/structured/json/{item['post_id']}.json"
            _write_json(BUCKET, out_key, item)
            written += 1
            done.add(name)
            processed_now.append(name)

        except Exception as e:
            errors += 1
            logging.error(f"[extractor] failed {name}: {e}\n{traceback.format_exc()}")
            # mark it processed so we don't loop forever; you can change this to retry logic if you want
            done.add(name)
            processed_now.append(name)

    _write_state(BUCKET, STATE_KEY, done)

    result = {"processed": len(processed_now), "written_json": written, "errors": errors}
    logging.info(json.dumps(result))
    return jsonify(result), 200
