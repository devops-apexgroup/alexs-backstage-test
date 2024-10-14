# Makefile
.DEFAULT_GOAL := help

POETRY_BIN :=  $(shell which poetry)

DAG_NAME := $(shell basename $(PWD))
ENV_PATH :=../../dependencies/$(DAG_NAME)/

# Define colors
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
CYAN := \033[0;36m
PURPLE := \033[0;35m
RED := \033[0;31m
NC := \033[0m

.PHONY: help
help: ## 🌟 Show available targets
	@echo "$(GREEN)Available Targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "$(CYAN)%-15s$(NC) $(PURPLE)%-15s$(NC) %s\n", $$1, $$2, $$3}' $(MAKEFILE_LIST)

.PHONY: attach
attach: ## 🚀 Refresh Dag-bag and force the scheduler to check new updates in Dag workdir. 
	@$(POETRY_BIN) config virtualenvs.in-project false --local
	@$(POETRY_BIN) config virtualenvs.path $(ENV_PATH) --local
	@$(POETRY_BIN) install --no-root
