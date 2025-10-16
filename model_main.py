# Cloud Functions (Gen2) HTTP entrypoint:
# - Loads ALL structured JSON from gs://<bucket>/<prefix>/*/structured/json/*.json
# - Identifies the NEWEST run_id (folder), trains on prior runs, predicts on newest
# - Writes predictions CSV + metrics back to GCS
#
# Outputs:
#   gs://<bucket>/<prefix>/predictions/<run_id>_predictions.csv
#   gs://<bucket>/<prefix>/metrics/history.csv  (appended)

import os, io, json, re
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

import pandas as pd
from google.cloud import storage
from flask import Request, jsonify

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.tree import DecisionTreeRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error

PROJECT_ID = os.getenv("PROJECT_ID", "craigslist-scraper-v2")
BUCKET = os.getenv("GCS_BUCKET")  # REQUIRED: same as scraper bucket
PREFIX = os.getenv("OUTPUT_PREFIX", "craigslist")

def storage_client():
    return storage.Client()

def list_json_rows(bucket: str, prefix: str) -> pd.DataFrame:
    """Read all JSON objects under <prefix>/*/structured/json/*.json into a DataFrame."""
    client = storage_client()
    blobs = client.list_blobs(bucket, prefix=f"{prefix}/")
    rows = []
    for bl in blobs:
        # Expect: craigslist/<run_id>/structured/json/<post_id>.json
        if not bl.name.endswith(".json"): 
            continue
        if "/structured/json/" not in bl.name: 
            continue
        try:
            d = json.loads(bl.download_as_text())
            d["_gcs_path"] = bl.name
            # run_id is the 2nd segment (craigslist/<run_id>/...)
            parts = bl.name.split("/")
            d["_run_id"] = parts[1] if len(parts) > 1 else "unknown"
            rows.append(d)
        except Exception:
            # ignore bad records
            continue
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def write_gcs_text(bucket: str, key: str, text: str, content_type: str = "text/plain"):
    client = storage_client()
    blob = client.bucket(bucket).blob(key)
    blob.upload_from_string(text, content_type=content_type)
    return f"gs://{bucket}/{key}"

def append_csv_to_gcs(bucket: str, key: str, df: pd.DataFrame):
    client = storage_client()
    blob = client.bucket(bucket).blob(key)
    csv_buf = io.StringIO()
    if blob.exists():
        # append mode: download, concat, re-upload
        existing = pd.read_csv(io.StringIO(blob.download_as_text()))
        out = pd.concat([existing, df], ignore_index=True)
    else:
        out = df
    out.to_csv(csv_buf, index=False)
    blob.upload_from_string(csv_buf.getvalue(), content_type="text/csv")
    return f"gs://{bucket}/{key}"

def features_target(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    # Minimal feature set for now
    # normalize column names from extractor schema
    for col in ["year", "make", "model", "price"]:
        if col not in df.columns:
            df[col] = None
    # Keep only records with known price for training
    d = df.copy()
    d["year"] = pd.to_numeric(d["year"], errors="coerce")
    d["price"] = pd.to_numeric(d["price"], errors="coerce")
    d["make"] = d["make"].astype("string")
    d["model"] = d["model"].astype("string")
    d.loc[d["year"].isna(), "year"] = d["year"].median(skipna=True)

    X = d[["year", "make", "model"]]
    y = d["price"]
    return X, y

def build_pipeline() -> Pipeline:
    preproc = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), ["make", "model"])],
        remainder="passthrough",
    )
    pipe = Pipeline([
        ("prep", preproc),
        ("model", DecisionTreeRegressor(max_depth=6, random_state=42)),
    ])
    return pipe

def train_and_predict() -> Dict:
    if not BUCKET:
        return {"error": "GCS_BUCKET env var required"}

    df = list_json_rows(BUCKET, PREFIX)
    if df.empty:
        return {"message": "no structured data yet"}

    # Identify newest run (lexicographically works with YYYYMMDDTHHMMSSZ)
    run_ids = sorted(df["_run_id"].dropna().unique())
    newest = run_ids[-1]
    df_new = df[df["_run_id"] == newest].copy()
    df_hist = df[df["_run_id"] != newest].copy()

    # If no history yet, train on whatever has price (could include newest)
    train_df = df_hist if not df_hist.empty else df.copy()
    X_all, y_all = features_target(train_df)
    train_mask = y_all.notna()
    X_train, y_train = X_all[train_mask], y_all[train_mask]

    if X_train.empty:
        return {"message": "no labeled data (price) to train on yet"}

    model = build_pipeline()
    model.fit(X_train, y_train)

    # Predict on newest batch
    X_new, _ = features_target(df_new)
    preds = model.predict(X_new)

    out = df_new[["post_id", "url", "title", "make", "model", "year"]].copy()
    out["actual_price"] = pd.to_numeric(df_new.get("price"), errors="coerce")
    out["pred_price"] = preds
    out["abs_error"] = (out["actual_price"] - out["pred_price"]).abs()

    # Save predictions CSV
    pred_key = f"{PREFIX}/predictions/{newest}_predictions.csv"
    pred_uri = write_gcs_text(BUCKET, pred_key, out.to_csv(index=False), "text/csv")

    # Compute simple metric where actual exists
    eval_df = out.dropna(subset=["actual_price"])
    mae = float(mean_absolute_error(eval_df["actual_price"], eval_df["pred_price"])) if not eval_df.empty else None
    metrics_row = pd.DataFrame([{
        "run_id": newest,
        "rows_scored": int(len(out)),
        "rows_with_actual": int(len(eval_df)),
        "mae": mae,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }])

    # Append to metrics history
    metrics_key = f"{PREFIX}/metrics/history.csv"
    metrics_uri = append_csv_to_gcs(BUCKET, metrics_key, metrics_row)

    return {
        "newest_run": newest,
        "predictions_csv": pred_uri,
        "metrics_csv": metrics_uri,
        "rows_scored": int(len(out)),
        "rows_with_actual": int(len(eval_df)),
        "mae": mae
    }

def train_http(request: Request):
    try:
        result = train_and_predict()
        status = 200 if "error" not in result else 500
        return jsonify(result), status
    except Exception as e:
        return jsonify({"error": str(e)}), 500
