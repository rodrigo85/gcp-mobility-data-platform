-include .env
export

PYTHON ?= python3.11
VENVS  := .venvs
ifeq ($(OS),Windows_NT)
  BIN := Scripts
else
  BIN := bin
endif

BEAM_PY  := $(VENVS)/beam/$(BIN)/python
SPARK_PY := $(VENVS)/spark/$(BIN)/python
SIM_PY   := $(VENVS)/simulator/$(BIN)/python
DBT      := $(VENVS)/dbt/$(BIN)/dbt
TF       := terraform -chdir=infra/terraform

MONTH ?= 2025-01

.PHONY: help venvs lint test test-beam test-spark test-simulator dbt-parse tf-check check \
        tf-init tf-plan tf-apply tf-validate \
        emulator-topics simulate beam-local beam-dataflow \
        vps-deploy vps-setup vps-dag-test vps-download vps-spark-normalize \
        dbt-deps dbt-seed dbt-build dbt-docs

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

# --- Environments ------------------------------------------------------------
venvs: ## Create one virtualenv per component
	$(PYTHON) -m venv $(VENVS)/beam      && $(BEAM_PY)  -m pip install -r pipelines/beam/requirements.txt -e pipelines/beam
	$(PYTHON) -m venv $(VENVS)/spark     && $(SPARK_PY) -m pip install -r pipelines/spark/requirements.txt
	$(PYTHON) -m venv $(VENVS)/simulator && $(SIM_PY)   -m pip install -r simulator/requirements.txt
	$(PYTHON) -m venv $(VENVS)/dbt       && $(VENVS)/dbt/$(BIN)/python -m pip install -r dbt/requirements.txt

# --- Quality -----------------------------------------------------------------
lint: ## Ruff + SQLFluff + terraform fmt
	$(BEAM_PY) -m ruff check . && $(BEAM_PY) -m ruff format --check .
	$(VENVS)/dbt/$(BIN)/sqlfluff lint dbt/models dbt/analyses
	$(TF) fmt -check -recursive

test: test-simulator test-beam test-spark ## Unit tests for every component

test-beam:
	cd pipelines/beam && ../../$(BEAM_PY) -m pytest tests

test-spark:
	cd pipelines/spark && ../../$(SPARK_PY) -m pytest tests

test-simulator:
	cd simulator && ../$(SIM_PY) -m pytest tests

dbt-parse: ## Validate the dbt project without a BigQuery connection
	cd dbt && GCP_PROJECT_ID=$${GCP_PROJECT_ID:-offline-check} DBT_PROFILES_DIR=. ../$(DBT) deps
	cd dbt && GCP_PROJECT_ID=$${GCP_PROJECT_ID:-offline-check} DBT_PROFILES_DIR=. ../$(DBT) parse --warn-error

tf-check: ## terraform fmt + validate without credentials or backend
	$(TF) fmt -check -recursive
	$(TF) init -backend=false -input=false
	$(TF) validate

check: test dbt-parse tf-check ## Everything that runs offline: no GCP account needed

# --- Infra -------------------------------------------------------------------
tf-init: ## terraform init
	$(TF) init
tf-validate:
	$(TF) validate
tf-plan: ## terraform plan
	$(TF) plan -var="project_id=$(GCP_PROJECT_ID)" -out=tfplan
tf-apply: ## Apply the last saved plan
	$(TF) apply tfplan

# --- Streaming ---------------------------------------------------------------
emulator-topics: ## Create topic and subscription on the Pub/Sub emulator
	PYTHONPATH=simulator $(SIM_PY) -m ride_simulator.emulator --project $(GCP_PROJECT_ID) \
		--topic $(PUBSUB_TOPIC) --subscription $(PUBSUB_SUBSCRIPTION)

simulate: ## Publish ride events to Pub/Sub (or the emulator)
	PYTHONPATH=simulator $(SIM_PY) -m ride_simulator --project $(GCP_PROJECT_ID) --topic $(PUBSUB_TOPIC) \
		--rate 20 --duration-seconds 300

beam-local: ## Streaming pipeline on the DirectRunner, output to the log
	$(BEAM_PY) -m mobility_beam.streaming_rides --runner DirectRunner --streaming \
		--input_subscription projects/$(GCP_PROJECT_ID)/subscriptions/$(PUBSUB_SUBSCRIPTION) \
		--sink log

beam-dataflow: ## Same pipeline on Dataflow (costs money: use trial credits)
	$(BEAM_PY) -m mobility_beam.streaming_rides --runner DataflowRunner --streaming \
		--project $(GCP_PROJECT_ID) --region $(GCP_REGION) \
		--temp_location gs://$(GCS_BUCKET)/dataflow/tmp \
		--setup_file pipelines/beam/setup.py \
		--max_num_workers 1 --machine_type e2-small \
		--input_subscription projects/$(GCP_PROJECT_ID)/subscriptions/$(PUBSUB_SUBSCRIPTION) \
		--sink bigquery --bq_project $(GCP_PROJECT_ID)

# --- VPS (Airflow 2.10 + Spark 3.5; see docs/deploy-vps.md) ----------------
# Run these targets from a shell with SSH access to the VPS (on Windows, inside WSL).
VPS          ?= vpsmouts
VPS_DIR      ?= /opt/spark-cluster/projects/gcp-mobility-data-platform
PIPELINE_DIR := /pipeline/gcp-mobility-data-platform
SPARK_PYTHON := /usr/local/bin/python3.11

vps-deploy: ## Sync the repository to the VPS
	rsync -az --delete --exclude-from=.vpsignore ./ $(VPS):$(VPS_DIR)/
	ssh $(VPS) 'mkdir -p $(VPS_DIR)/data $(VPS_DIR)/.venvs $(VPS_DIR)/.secrets && \
		chmod 700 $(VPS_DIR)/.secrets'

# The scheduler mounts the project read-only. The venvs are created in the
# Spark master (read-write), which has the same Python 3.11 under /usr/local,
# so the dbt venv runs the same in both containers.
vps-setup: ## Isolated venvs: dbt (containers' Python 3.11) and Beam (host Python)
	ssh $(VPS) 'docker exec da-spark-master sh -c "\
		$(SPARK_PYTHON) -m venv $(PIPELINE_DIR)/.venvs/dbt && \
		$(PIPELINE_DIR)/.venvs/dbt/bin/pip install -q -r $(PIPELINE_DIR)/dbt/requirements.txt && \
		cd $(PIPELINE_DIR)/dbt && ../.venvs/dbt/bin/dbt deps"'
	ssh $(VPS) 'python3 -m venv $(VPS_DIR)/.venvs/beam && \
		$(VPS_DIR)/.venvs/beam/bin/pip install -q -r $(VPS_DIR)/pipelines/beam/requirements.txt \
			-r $(VPS_DIR)/simulator/requirements.txt -e $(VPS_DIR)/pipelines/beam'

vps-dag-test: ## DAG integrity tests on the VPS Airflow
	ssh $(VPS) 'docker exec da-airflow-scheduler sh -c "\
		python -m pip install -q --target /tmp/mobility-pytest pytest==9.1.1 && \
		cd $(PIPELINE_DIR) && PYTHONPATH=/tmp/mobility-pytest python -m pytest \
			orchestration/airflow/tests -p no:cacheprovider"'

vps-download: ## Download one TLC month on the VPS: make vps-download MONTH=2025-01
	ssh $(VPS) 'docker exec da-spark-master $(SPARK_PYTHON) \
		$(PIPELINE_DIR)/pipelines/spark/mobility_spark/download_tlc.py \
		--month $(MONTH) --dest $(PIPELINE_DIR)/data/landing/yellow'

vps-spark-normalize: ## spark-submit on the cluster: make vps-spark-normalize MONTH=2025-01
	ssh $(VPS) 'docker exec da-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 --name normalize_tlc_$(MONTH) \
		--conf spark.executor.memory=4g --conf spark.executor.cores=2 \
		--conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=16 \
		$(PIPELINE_DIR)/pipelines/spark/mobility_spark/normalize_tlc.py --month $(MONTH) \
		--input $(PIPELINE_DIR)/data/landing/yellow/yellow_tripdata_$(MONTH).parquet \
		--output $(PIPELINE_DIR)/data/normalized/yellow_trips'

# --- dbt ---------------------------------------------------------------------
dbt-deps:
	cd dbt && ../$(DBT) deps
dbt-seed:
	cd dbt && ../$(DBT) seed
dbt-build: ## build = run + test + snapshot + seed, in dependency order
	cd dbt && ../$(DBT) build
dbt-docs:
	cd dbt && ../$(DBT) docs generate
