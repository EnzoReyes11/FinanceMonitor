PROJECT_ID := $(shell cd terraform/gcp_project && terraform output -raw project_id 2>/dev/null || echo "unknown")
REGION := us-central1

SERVICES := alphavantage_extractor iol_extractor alphavantage_loader

SERVICE_PATH = data_pipelines/stocks/services/$(1)

.PHONY: help clean update-project update-services
.PHONY: $(addprefix build-, $(SERVICES))
.PHONY: $(addprefix deploy-, $(SERVICES))
.PHONY: $(addprefix test-, $(SERVICES))
.PHONY: build-all deploy-all test-all

# Default target
help:
	@echo "Available targets:"
	@echo "  make build-<service>     - Build a specific service"
	@echo "  make deploy-<service>    - Deploy a specific service"
	@echo "  make test-<service>      - Test a specific service"
	@echo "  make build-all           - Build all services"
	@echo "  make deploy-all          - Deploy all services"
	@echo "  make test-all            - Test all services"
	@echo ""
	@echo "Services: $(SERVICES)"
	@echo ""
	@echo "Examples:"
	@echo "  make build-alphavantage-extractor"
	@echo "  make deploy-all"

# Infrastructure targets
update-project:
	@echo "📦 Updating GCP project infrastructure..."
	cd terraform/gcp_project && terraform apply -auto-approve

update-services:
	@echo "🚀 Updating Cloud Run services..."
	cd terraform/services && terraform apply -auto-approve

# Pattern rule: build-<service>
build-%:
	@echo "🔨 Building $*..."
	@if [ ! -f "$(call SERVICE_PATH,$*)/cloudbuild.yaml" ]; then \
		echo "❌ Error: cloudbuild.yaml not found for $*"; \
		exit 1; \
	fi
	gcloud builds submit \
		--region=$(REGION) \
		--config $(call SERVICE_PATH,$*)/cloudbuild.yaml \
		.

# Pattern rule: deploy-<service> (build then update)
deploy-%: build-% update-services
	@echo "✅ Deployed $*"

# Pattern rule: test-<service>
test-%:
	@echo "🧪 Testing $*..."
	gcloud run jobs execute $* \
		--region=$(REGION) \
		--wait

# Aggregate targets using loops
build-all:
	@echo "🔨 Building all services..."
	@$(foreach service,$(SERVICES), \
		echo "Building $(service)..." && \
		$(MAKE) build-$(service) || exit 1; \
	)
	@echo "✅ All services built!"

deploy-all:
	@echo "🚀 Deploying all services..."
	@$(foreach service,$(SERVICES), \
		echo "Deploying $(service)..." && \
		$(MAKE) deploy-$(service) || exit 1; \
	)
	@echo "✅ All services deployed!"

test-all:
	@echo "🧪 Testing all services..."
	@$(foreach service,$(SERVICES), \
		echo "Testing $(service)..." && \
		$(MAKE) test-$(service) || exit 1; \
	)
	@echo "✅ All services tested!"

# Full pipeline for all services
full-deploy-all: build-all update-services test-all
	@echo "✅ Complete deployment pipeline finished!"

# Utility targets
clean:
	@echo "🧹 Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true

# List all services
list-services:
	@echo "Configured services:"
	@for service in $(SERVICES); do \
		echo "  - $$service"; \
	done