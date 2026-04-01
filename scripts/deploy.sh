#!/usr/bin/env bash
# =============================================================
# deploy.sh — One-shot deployment script
# Usage: ./scripts/deploy.sh --project-id my-project --env dev
# =============================================================
set -euo pipefail

# ── Parse args ───────────────────────────────────────────────
PROJECT_ID=""
ENV="dev"
REGION="us-central1"
SKIP_INFRA=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --env)        ENV="$2";        shift 2 ;;
    --region)     REGION="$2";    shift 2 ;;
    --skip-infra) SKIP_INFRA=true; shift ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$PROJECT_ID" ]]; then
  echo "ERROR: --project-id is required"
  exit 1
fi

CYAN='\033[0;36m'
GREEN='\033[0;32m'
RESET='\033[0m'

log() { echo -e "${CYAN}[deploy]${RESET} $*"; }
ok()  { echo -e "${GREEN}[OK]${RESET} $*"; }

log "Deploying NYC Taxi Data Platform"
log "Project: $PROJECT_ID | Env: $ENV | Region: $REGION"

# ── 1. Validate gcloud auth ──────────────────────────────────
log "Checking gcloud authentication..."
gcloud auth print-access-token > /dev/null 2>&1 || {
  echo "Not authenticated. Run: gcloud auth application-default login"
  exit 1
}
ok "gcloud authenticated"

# ── 2. Enable APIs ───────────────────────────────────────────
log "Enabling required GCP APIs..."
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  workflows.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  --project="$PROJECT_ID" --quiet
ok "APIs enabled"

# ── 3. Create Terraform state bucket (idempotent) ────────────
STATE_BUCKET="${PROJECT_ID}-tf-state"
log "Ensuring Terraform state bucket: gs://$STATE_BUCKET"
gsutil ls -b "gs://$STATE_BUCKET" > /dev/null 2>&1 || \
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$STATE_BUCKET"
ok "State bucket ready"

# ── 4. Terraform ─────────────────────────────────────────────
if [[ "$SKIP_INFRA" == "false" ]]; then
  log "Running Terraform..."
  cd terraform

  # Update state bucket in main.tf if needed
  sed -i.bak "s/REPLACE_WITH_TF_STATE_BUCKET/$STATE_BUCKET/" main.tf 2>/dev/null || true

  terraform init -upgrade -reconfigure \
    -backend-config="bucket=$STATE_BUCKET"

  terraform apply \
    -var="project_id=$PROJECT_ID" \
    -var="env=$ENV" \
    -auto-approve

  ok "Infrastructure deployed"
  cd ..
fi

# ── 5. Install Python deps ───────────────────────────────────
log "Installing Python dependencies..."
pip install -q -r ingestion/requirements.txt
pip install -q -r quality/requirements.txt
ok "Python dependencies installed"

# ── 6. Summary ───────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${GREEN} Deployment complete!${RESET}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""
echo "  Next steps:"
echo "  1. Run the pipeline:"
echo "     make run-pipeline DATE=2024-01 PROJECT_ID=$PROJECT_ID ENV=$ENV"
echo ""
echo "  2. Check execution status:"
echo "     make pipeline-status PROJECT_ID=$PROJECT_ID ENV=$ENV"
echo ""
