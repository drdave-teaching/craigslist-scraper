import os, pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor

PROJECT_ID  = os.environ["PROJECT_ID"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "us")
TRAIN_SQL   = os.environ["TRAIN_SQL"]     # yesterday rows
PREDICT_SQL = os.environ["PREDICT_SQL"]   # today rows
PREDS_TABLE = os.environ["PREDS_TABLE"]   # e.g. project.dataset.todays_car_preds
TARGET      = os.environ.get("TARGET", "price")

bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

def read(sql):
    return bq.query(sql).result().to_dataframe(create_bqstorage_client=True)

print("→ loading train (yesterday)…")
train = read(TRAIN_SQL)
if train.empty:
    raise SystemExit("No training rows for yesterday; aborting.")

# simple numeric-only baseline
X = train.select_dtypes(include="number").drop(columns=[TARGET], errors="ignore")
y = train[TARGET]
print(f"train rows={len(train)}, features={len(X.columns)}")

model = RandomForestRegressor(n_estimators=200, n_jobs=-1, random_state=0)
model.fit(X, y)

print("→ loading predict set (today)…")
today = read(PREDICT_SQL)
if today.empty:
    raise SystemExit("No rows to predict for today; aborting.")

# align today’s columns to train feature set (fill missing with 0)
Xt = today.select_dtypes(include="number").reindex(columns=X.columns, fill_value=0)
preds = model.predict(Xt)

out = pd.DataFrame({
    "post_id": today["post_id"] if "post_id" in today.columns else pd.Series(range(len(today))),
    "pred_price": preds
})
# helpful breadcrumbs
out["model_name"] = "rf_baseline"
out["model_run_ts"] = pd.Timestamp.utcnow()

print(f"→ writing {len(out)} rows to {PREDS_TABLE} (replace)…")
out.to_gbq(PREDS_TABLE, project_id=PROJECT_ID, if_exists="replace")
print("✓ done")
