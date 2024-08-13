.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV = .venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

ifneq ($(TERM),)
    GREEN        := $(shell tput setaf 2)
    RESET        := $(shell tput sgr0)
else
    GREEN        := ""
    RESET        := ""
endif

.venv:  ## Set up virtual environment and install requirements
	pip install --upgrade pip
	pip install uv==0.2.35
	uv venv
	$(MAKE) requirements

.PHONY: requirements
requirements: .venv  ## Install/refresh all project requirements
	uv pip install -r requirements.txt \
		-e libraries/dagster-delta \
		-e libraries/dagster-delta-polars \
		--config-settings editable_mode=compat
		
.PHONY: pre-commit
pre-commit: .venv  ## Run autoformatting and linting
	@echo "${GREEN}Formatting with ruff...${RESET}"
	$(VENV_BIN)/ruff format .
	@echo "${GREEN}Linting with ruff...${RESET}"
	$(VENV_BIN)/ruff check .
	@echo "${GREEN}Running static type checks...${RESET}"
	$(VENV_BIN)/pyright .

.PHONY: ci-check
ci-check: .venv  ## Checks autoformatting and linting
	@echo "${GREEN}Checking formatting with ruff...${RESET}"
	$(VENV_BIN)/ruff format --check .
	@echo "${GREEN}Linting with ruff...${RESET}"
	$(VENV_BIN)/ruff check .
	@echo "${GREEN}Running static type checks...${RESET}"
	$(VENV_BIN)/pyright .

.PHONY: clean
clean: ## Remove environment and the caches
	@rm -rf .venv/
	@rm -rf .mypy_cache/
	@rm -rf .pytest_cache/
	@rm -rf .ruff_cache/

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort