.PHONY: all fmt lint test build clean help

RUFF ?= ruff
CARGO ?= cargo

all: fmt lint test build ## Run everything (fmt, lint, test, build)

fmt: fmt-rust fmt-python ## Format all code

fmt-rust: ## Format Rust code
	$(CARGO) fmt

fmt-python: ## Format Python code
	$(RUFF) format .

lint: lint-rust lint-python ## Lint all code

lint-rust: ## Lint Rust code
	$(CARGO) clippy -- -D warnings

lint-python: ## Lint Python code
	$(RUFF) check .

test: test-rust ## Run all tests

test-rust: ## Run Rust tests
	$(CARGO) test

build: build-rust ## Build the project

build-rust: ## Build Rust project
	$(CARGO) build

clean: ## Clean build artifacts
	$(CARGO) clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
