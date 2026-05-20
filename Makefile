.DEFAULT_GOAL := help

VERSION ?= dev
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -s -w \
	-X github.com/sentinelhq-sh/outpost/internal/version.Version=$(VERSION) \
	-X github.com/sentinelhq-sh/outpost/internal/version.Commit=$(COMMIT)

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the local binary into ./bin/outpost
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/outpost ./cmd/outpost

.PHONY: test
test: ## Run all Go tests
	go test ./...

.PHONY: lint
lint: ## golangci-lint + gofmt check
	golangci-lint run ./...
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run 'gofmt -w .'"; exit 1)

.PHONY: run
run: build ## Build and run locally (loads SENTINELHQ_* from env)
	./bin/outpost

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin
