.PHONY: help build test lint format clean vet tidy tools

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

tools: ## Install required tools
	@echo "Installing tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.3
	@go install golang.org/x/tools/cmd/goimports@latest
	@echo "Tools installed successfully"

build: ## Build the project
	@echo "Building..."
	@go build -v ./...

test: ## Run tests
	@echo "Running tests..."
	@go test -v -race -coverprofile=coverage.out -covermode=atomic ./...

test-coverage: test ## Run tests and show coverage
	@go tool cover -html=coverage.out

lint: ## Run linters
	@echo "Running linters..."
	@golangci-lint run ./...

format: ## Format code
	@echo "Formatting code..."
	@gofmt -s -w .

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

tidy: ## Tidy dependencies
	@echo "Tidying dependencies..."
	@go mod tidy

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -f coverage.out
	@go clean -cache -testcache

ci: tidy format vet lint test ## Run all CI checks (format, vet, lint, test)

.DEFAULT_GOAL := help
