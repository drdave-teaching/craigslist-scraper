# Deploy from GitHub (Path B)
# 1) One-time infra (Cloud Shell)

```python
# ---- basics ----
PROJECT_ID="craigslist-scraper-v2"
REGION="us-central1"
BUCKET="craigslist-data-$PROJECT_ID"
REPO="drdave-teaching/craigslist-scraper"   # <owner>/<repo>
gcloud config set project $PROJECT_ID

# ---- APIs ----
gcloud services enable \
  cloudfunctions.googleapis.com run.googleapis.com \
  artifactregistry.googleapis.com cloudscheduler.googleapis.com \
  iamcredentials.googleapis.com cloudbuild.googleapis.com

# ---- service accounts ----
gcloud iam service-accounts create sa-scraper   --display-name="Craigslist runtime SA"
gcloud iam service-accounts create sa-scheduler --display-name="Scheduler invoker SA"
gcloud iam service-accounts create sa-deployer  --display-name="GitHub Actions Deployer"

RUNTIME_SA="sa-scraper@$PROJECT_ID.iam.gserviceaccount.com"
SCHEDULER_SA="sa-scheduler@$PROJECT_ID.iam.gserviceaccount.com"
DEPLOYER_SA="sa-deployer@$PROJECT_ID.iam.gserviceaccount.com"

# ---- bucket for outputs ----
gsutil mb -l $REGION gs://$BUCKET
gsutil iam ch serviceAccount:$RUNTIME_SA:roles/storage.objectAdmin gs://$BUCKET

# ---- Gen2 first-deploy bindings (standard) ----
PROJECT_NUMBER="$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')"
CLOUD_BUILD_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"
CLOUD_RUN_SA="service-${PROJECT_NUMBER}@serverless-robot-prod.iam.gserviceaccount.com"
CLOUD_FUNCTIONS_SA="service-${PROJECT_NUMBER}@gcf-admin-robot.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --member="serviceAccount:$CLOUD_BUILD_SA" --role="roles/iam.serviceAccountUser"
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --member="serviceAccount:$CLOUD_RUN_SA"   --role="roles/iam.serviceAccountUser"
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --member="serviceAccount:$CLOUD_FUNCTIONS_SA" --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$CLOUD_BUILD_SA" --role="roles/artifactregistry.writer"

```
## Workload Identity Federation (link GitHub → GCP)
```python
# ---- OIDC pool + provider for your repo ----
POOL_ID="github-pool"
PROVIDER_ID="github-provider"

gcloud iam workload-identity-pools create "$POOL_ID" \
  --location="global" --display-name="GitHub OIDC Pool"

gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
  --location="global" \
  --workload-identity-pool="$POOL_ID" \
  --display-name="GitHub Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref,attribute.actor=assertion.actor,attribute.workflow=assertion.workflow" \
  --attribute-condition="attribute.repository=='$REPO'"

WORKLOAD_IDENTITY_PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"
echo "$WORKLOAD_IDENTITY_PROVIDER"

# ---- allow GitHub (via provider) to impersonate the deployer SA ----
gcloud iam service-accounts add-iam-policy-binding "$DEPLOYER_SA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository/${REPO}"

# ---- give the deployer SA the least-priv deploy roles ----
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DEPLOYER_SA" --role="roles/cloudfunctions.developer"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DEPLOYER_SA" --role="roles/run.admin"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DEPLOYER_SA" --role="roles/artifactregistry.writer"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DEPLOYER_SA" --role="roles/cloudbuild.builds.editor"

# deployer SA may set function to run as runtime SA
gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --member="serviceAccount:$DEPLOYER_SA" --role="roles/iam.serviceAccountUser"
```

# 2) Repo → add Actions Variables (GitHub → Settings → Secrets and variables → Actions → Variables)
Set these (no secrets/keys):
* PROJECT_ID = `craigslist-scraper-v2`
* REGION = `us-central1`
* WORKLOAD_IDENTITY_PROVIDER = `<paste value echoed above>`
* DEPLOYER_SA = `sa-deployer@craigslist-scraper-v2.iam.gserviceaccount.com`
* RUNTIME_SA = `sa-scraper@craigslist-scraper-v2.iam.gserviceaccount.com`
* SCHEDULER_SA = `sa-scheduler@craigslist-scraper-v2.iam.gserviceaccount.com`
* GCS_BUCKET = `craigslist-data-craigslist-scraper-v2`

# 3) Commit workflow: `.github/workflows/deploy.yml`
```yaml
name: Deploy — Craigslist Scraper (Cloud Functions Gen2)

on:
  push:
    branches: [ main ]
    paths:
      - 'main.py'
      - 'requirements.txt'
      - '.github/workflows/deploy.yml'
  workflow_dispatch:

permissions:
  contents: read
  id-token: write   # OIDC

env:
  PROJECT_ID: ${{ vars.PROJECT_ID }}
  REGION: ${{ vars.REGION }}
  WORKLOAD_IDENTITY_PROVIDER: ${{ vars.WORKLOAD_IDENTITY_PROVIDER }}
  DEPLOYER_SA: ${{ vars.DEPLOYER_SA }}
  RUNTIME_SA: ${{ vars.RUNTIME_SA }}
  SCHEDULER_SA: ${{ vars.SCHEDULER_SA }}
  FUNCTION_NAME: craigslist-scraper
  GCS_BUCKET: ${{ vars.GCS_BUCKET }}
  OUTPUT_PREFIX: craigslist
  BASE_SITE: https://newhaven.craigslist.org
  SEARCH_PATH: /search/cta
  MAX_PAGES: '3'
  REQUEST_DELAY_SECS: '1.0'
  DETAIL_REQUEST_DELAY_SECS: '1.0'
  JOB_ID: craigslist-scraper-6h
  CRON: '0 */6 * * *'
  TIMEZONE: 'America/New_York'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Auth to GCP via OIDC
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ env.WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ env.DEPLOYER_SA }}
          project_id: ${{ env.PROJECT_ID }}

      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v2

      - name: Enable required services (idempotent)
        run: |
          gcloud services enable \
            cloudfunctions.googleapis.com run.googleapis.com \
            artifactregistry.googleapis.com cloudscheduler.googleapis.com

      - name: Deploy Cloud Function (Gen2)
        run: |
          gcloud functions deploy "$FUNCTION_NAME" \
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
            --set-env-vars="GCS_BUCKET=$GCS_BUCKET,OUTPUT_PREFIX=$OUTPUT_PREFIX,BASE_SITE=$BASE_SITE,SEARCH_PATH=$SEARCH_PATH,MAX_PAGES=$MAX_PAGES,REQUEST_DELAY_SECS=$REQUEST_DELAY_SECS,DETAIL_REQUEST_DELAY_SECS=$DETAIL_REQUEST_DELAY_SECS"

      - name: Get function URL
        id: fn
        run: |
          URL="$(gcloud functions describe "$FUNCTION_NAME" --region="$REGION" --format='value(serviceConfig.uri)')"
          echo "FUNCTION_URL=$URL" >> "$GITHUB_OUTPUT"

      - name: Grant run.invoker to Scheduler SA (idempotent)
        run: |
          gcloud run services add-iam-policy-binding "$FUNCTION_NAME" \
            --region="$REGION" \
            --member="serviceAccount:${{ env.SCHEDULER_SA }}" \
            --role="roles/run.invoker" || true

      - name: Create/Update Cloud Scheduler job (OIDC)
        run: |
          gcloud scheduler jobs create http "$JOB_ID" \
            --location="$REGION" \
            --schedule="$CRON" \
            --time-zone="$TIMEZONE" \
            --http-method=POST \
            --uri="${{ steps.fn.outputs.FUNCTION_URL }}" \
            --oidc-service-account-email="$SCHEDULER_SA" \
            --oidc-token-audience="${{ steps.fn.outputs.FUNCTION_URL }}" \
            --headers="Content-Type=application/json" \
            --message-body='{"max_pages": 3, "prefix": "craigslist"}' \
          || gcloud scheduler jobs update http "$JOB_ID" \
            --location="$REGION" \
            --schedule="$CRON" \
            --time-zone="$TIMEZONE" \
            --http-method=POST \
            --uri="${{ steps.fn.outputs.FUNCTION_URL }}" \
            --oidc-service-account-email="$SCHEDULER_SA" \
            --oidc-token-audience="${{ steps.fn.outputs.FUNCTION_URL }}" \
            --headers="Content-Type=application/json" \
            --message-body='{"max_pages": 3, "prefix": "craigslist"}'
```

# 4) Deploy = push
* Push to main → check Actions tab.
* After it finishes: the function is private, the Scheduler will call it every 6 hours with OIDC.
* Outputs land in:
`gs://craigslist-data-craigslist-scraper-v2/craigslist/<YYYYMMDDTHHMMSSZ>/index.csv and txt/*.txt.`

# 5) Quick verification
```bash
# manual kick (from Cloud Shell)
gcloud scheduler jobs run craigslist-scraper-6h --location=us-central1

# list newest outputs
gsutil ls -r gs://craigslist-data-craigslist-scraper-v2/craigslist/ | tail -n 50

# see recent logs
gcloud functions logs read craigslist-scraper --region=us-central1 --limit=100
```
