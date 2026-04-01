# ============================================================
# NYC Taxi Data Platform — Makefile
# ============================================================
# Usage:
#   make help              Show all available targets
#   make tf-init           Initialize Terraform
#   make tf-plan           Preview infrastructure changes
#   make tf-apply          Apply infrastructure
#   make tf-destroy        Destroy infrastructure
#   make deploy            Full deploy (infra + pipeline setup)
#   make ingest DATE=2024-01   Ingest a specific month
#   make run-pipeline      Trigger the full Workflows pipeline
#   make quality-check     Run data quality validations
#   make lint              Lint Python code
#   make test              Run tests

.PHONY: help tf-init tf-plan tf-apply tf-destroy deploy ingest run-pipeline quality-check lint test fmt

# ── Config ──────────────────────────────────────────────────
PROJECT_ID     ?= $(shell gcloud config get-value project 2>/dev/null)
REGION         ?= us-central1
ENV            ?= dev
TF_DIR         := terraform
PYTHON         := python3
PIP            := pip3
DATE           ?= $(shell date +%Y-%m --date="-1 month")
WORKFLOW_NAME  ?= nyc-taxi-pipeline-$(ENV)

# ── Colors ──────────────────────────────────────────────────
CYAN  := \033[0;36m
RESET := \033[0m

## help: Show this help message
help:
	@echo ""
	@echo "$(CYAN)NYC Taxi Data Platform$(RESET)"
	@echo "────────────────────────────────────────"
	@grep -E '^## ' Makefile | sed 's/## /  /'
	@echo ""

# ── Terraform ───────────────────────────────────────────────

## tf-init: Initialize Terraform and download providers
tf-init:
	@echo "$(CYAN)→ Initializing Terraform...$(RESET)"
	cd $(TF_DIR) && terraform init -upgrade

## tf-validate: Validate Terraform configuration
tf-validate:
	@echo "$(CYAN)→ Validating Terraform...$(RESET)"
	cd $(TF_DIR) && terraform validate

## tf-fmt: Format Terraform files
tf-fmt:
	@echo "$(CYAN)→ Formatting Terraform...$(RESET)"
	cd $(TF_DIR) && terraform fmt -recursive

## tf-plan: Show infrastructure plan
tf-plan: tf-validate
	@echo "$(CYAN)→ Planning Terraform (env=$(ENV))...$(RESET)"
	cd $(TF_DIR) && terraform plan \
		-var="project_id=$(PROJECT_ID)" \
		-var="env=$(ENV)" \
		-out=terraform.plan

## tf-apply: Apply infrastructure changes
tf-apply:
	@echo "$(CYAN)→ Applying Terraform (env=$(ENV))...$(RESET)"
	cd $(TF_DIR) && terraform apply \
		-var="project_id=$(PROJECT_ID)" \
		-var="env=$(ENV)" \
		-auto-approve

## tf-destroy: Destroy all infrastructure (use with caution)
tf-destroy:
	@echo "$(CYAN)→ Destroying infrastructure (env=$(ENV))...$(RESET)"
	@read -p "Are you sure? This will delete all resources. (yes/no): " confirm && \
		[ "$$confirm" = "yes" ] || exit 1
	cd $(TF_DIR) && terraform destroy \
		-var="project_id=$(PROJECT_ID)" \
		-var="env=$(ENV)" \
		-auto-approve

## tf-output: Show Terraform outputs
tf-output:
	cd $(TF_DIR) && terraform output -json

# ── Python / Ingestion ───────────────────────────────────────

## install: Install Python dependencies
install:
	@echo "$(CYAN)→ Installing dependencies...$(RESET)"
	$(PIP) install -r ingestion/requirements.txt
	$(PIP) install -r quality/requirements.txt
	$(PIP) install -r tests/requirements.txt

## ingest: Ingest data for a specific month (DATE=YYYY-MM)
ingest:
	@echo "$(CYAN)→ Ingesting data for $(DATE)...$(RESET)"
	$(PYTHON) ingestion/ingest.py \
		--project-id $(PROJECT_ID) \
		--execution-date $(DATE) \
		--env $(ENV)

## quality-check: Run data quality validations
quality-check:
	@echo "$(CYAN)→ Running quality checks ($(DATE))...$(RESET)"
	$(PYTHON) quality/run_checks.py \
		--project-id $(PROJECT_ID) \
		--execution-date $(DATE) \
		--env $(ENV)

# ── Workflows ────────────────────────────────────────────────

## run-pipeline: Trigger the full orchestrated pipeline via Google Workflows
run-pipeline:
	@echo "$(CYAN)→ Triggering pipeline for $(DATE)...$(RESET)"
	gcloud workflows run $(WORKFLOW_NAME) \
		--project=$(PROJECT_ID) \
		--location=$(REGION) \
		--data='{"execution_date": "$(DATE)", "env": "$(ENV)"}' \
		--format=json

## pipeline-status: List recent Workflow executions
pipeline-status:
	gcloud workflows executions list $(WORKFLOW_NAME) \
		--project=$(PROJECT_ID) \
		--location=$(REGION) \
		--limit=10 \
		--format="table(name.basename(), state, startTime, endTime)"

# ── Code Quality ─────────────────────────────────────────────

## lint: Lint Python code with ruff
lint:
	@echo "$(CYAN)→ Linting Python...$(RESET)"
	ruff check ingestion/ quality/

## fmt: Format Python code
fmt:
	@echo "$(CYAN)→ Formatting Python...$(RESET)"
	ruff format ingestion/ quality/

## test: Run unit tests (no GCP required)
test:
	@echo "$(CYAN)→ Running tests...$(RESET)"
	$(PYTHON) -m pytest

## test-fast: Run tests without coverage report
test-fast:
	@echo "$(CYAN)→ Running tests (fast)...$(RESET)"
	$(PYTHON) -m pytest --no-cov -q

# ── Deployment ───────────────────────────────────────────────

## deploy: Full deployment (infra + enable APIs)
deploy: tf-init tf-apply
	@echo "$(CYAN)→ Deployment complete!$(RESET)"
	@echo "Run 'make run-pipeline DATE=YYYY-MM' to start the pipeline."

## setup-gcp: Enable required GCP APIs (run once)
setup-gcp:
	@echo "$(CYAN)→ Enabling GCP APIs...$(RESET)"
	gcloud services enable \
		bigquery.googleapis.com \
		storage.googleapis.com \
		workflows.googleapis.com \
		cloudscheduler.googleapis.com \
		cloudresourcemanager.googleapis.com \
		iam.googleapis.com \
		logging.googleapis.com \
		monitoring.googleapis.com \
		--project=$(PROJECT_ID)
	@echo "$(CYAN)→ APIs enabled.$(RESET)"
