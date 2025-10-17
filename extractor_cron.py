# HTTP cron extractor for GCF Gen2. No Eventarc needed.
import os, io, json, re, time
from datetime import datetime, timezone
from typing import Dict, List, Set

from google.cloud import storage
from flask import Request, jsonify

BUCKET = os.getenv("GCS_BUCKET")
PREFIX = os.getenv("OUTPUT_PREFIX", "craigslist")
STATE_KEY = f"{PREFIX}/state/last_extracted.txt"   # stores processed blob names

POST_ID_RE = re.compile(r"/(\d{8,12})\.html")
PRICE_RE = re.compile(r"\$?([\d,]+)")

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
    return set(ln.strip() for ln in b.download_as_text().splitlines() if ln.strip())

def _write_state(bucket: str, key: str, processed: Set[str]):
    text = "\n".join(sorted(processed))
    _sc().bucket(bucket).blob(key).upload_from_string(text, content_type="text/plain")

def parse_txt(body: str) -> Dict:
    lines = body.splitlines()
    data = {"attrs_json": {}}
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("Title:"):
            data["title"] = line.split(":",1)[1].strip() or None
        elif line.startswith("Price:"):
            m = PRICE_RE.search(line); data["price"] = int(m.group(1).replace(",","")) if m else None
        elif line.startswith("Neighborhood:"):
            data["location"] = line.split(":",1)[1].strip() or None
        elif line.startswith("URL:"):
            data["url"] = line.split(":",1)[1].strip() or None
        elif line.startswith("Posted:"):
            data["posted_iso"] = line.split(":",1)[1].strip() or None
        elif line.startswith("Attributes:"):
            i += 1
            while i < len(lines) and lines[i].strip().startswith("-"):
                kv = lines[i].strip()[1:].strip()
                if ":" in kv:
                    k,v = kv.split(":",1); data["attrs_json"][k.strip().lower()] = v.strip()
                else:
                    misc = data["attrs_json"].get("misc", []); misc.append(kv)
                    data["attrs_json"]["misc"] = misc
                i += 1
            continue
        elif line == "BODY:" or line.endswith("BODY:"):
            data["body"] = "\n".join(lines[i+1:]).strip(); break
        i += 1

    # heuristics
    if data.get("title"):
        ym = re.search(r"\b(19|20)\d{2}\b", data["title"])
        data["year"] = int(ym.group(0)) if ym else None
        tail = re.sub(r"\b(19|20)\d{2}\b","", data["title"]).strip().split()
        data["make"]  = tail[0].title() if tail else None
        data["model"] = " ".join(tail[1:3]).title() if len(tail)>1 else None

    if data.get("url"):
        m = POST_ID_RE.search(data["url"])
        if m: data["post_id"] = m.group(1)

    return data

def extract_http(request: Request):
    if not BUCKET:
        return jsonify({"error":"missing GCS_BUCKET"}), 500

    # load state
    done = _read_state(BUCKET, STATE_KEY)

    # iterate new txt files
    sc = _sc()
    to_process: List[str] = []
    for blob in sc.list_blobs(BUCKET, prefix=f"{PREFIX}/"):
        n = blob.name
        if n.endswith(".txt") and "/txt/" in n and n not in done:
            to_process.append(n)

    processed_now: List[str] = []
    written = 0
    for name in to_process:
        text = _read_text(BUCKET, name)
        item = parse_txt(text)
        if not item.get("post_id"):
            m = re.search(r"(\d{8,12})", name)
            if m: item["post_id"] = m.group(1)
        if not item.get("post_id"):
            done.add(name); processed_now.append(name); continue

        # output path: craigslist/<run_id>/structured/json/<post_id>.json
        parts = name.split("/")
        run_prefix = "/".join(parts[:2])
        out_key = f"{run_prefix}/structured/json/{item['post_id']}.json"
        _write_json(BUCKET, out_key, item)
        written += 1
        done.add(name)
        processed_now.append(name)

    _write_state(BUCKET, STATE_KEY, done)
    return jsonify({"processed": len(processed_now), "written_json": written})
