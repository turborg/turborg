.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash

# Build flags mirror .goreleaser.yml so `make build` and a release
# binary differ only by version stamp.
VERSION ?= dev
LDFLAGS := -s -w -X github.com/turborg/turborg/internal/version.Version=$(VERSION)
GO_BUILD_FLAGS := -trimpath -ldflags="$(LDFLAGS)"

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS=":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build the turborg binary into ./bin/turborg.
	@mkdir -p bin
	CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) -o bin/turborg ./cmd/turborg

.PHONY: test
test: ## Run the full test suite with race detector.
	go test -race -count=1 -timeout 120s ./...

.PHONY: cover
cover: ## Run tests + write coverage profile + print summary.
	go test -race -count=1 -coverprofile=coverage.out ./internal/...
	@go tool cover -func=coverage.out | tail -1

.PHONY: cover-html
cover-html: cover ## Render the coverage profile as HTML at coverage.html.
	go tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

.PHONY: cover-gate
cover-gate: cover ## Enforce the configured per-package + total thresholds.
	go run github.com/vladopajic/go-test-coverage/v2@latest --config .testcoverage.yml

.PHONY: lint
lint: ## Run golangci-lint against the whole tree.
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format every Go file in place.
	gofmt -s -w .

.PHONY: vet
vet: ## go vet on every package.
	go vet ./...

.PHONY: tidy
tidy: ## Tidy go.mod + go.sum.
	go mod tidy

.PHONY: docker
docker: ## Build the production Docker image as turborg:dev.
	docker build -t turborg:dev --build-arg VERSION=$(VERSION) .

.PHONY: clean
clean: ## Remove build + coverage artifacts.
	rm -rf bin coverage.out coverage.html
