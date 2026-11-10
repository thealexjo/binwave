GOLANGCI_LINT_VERSION := v2.15.0

BIN_DIR   := $(CURDIR)/bin
BINARY    := $(BIN_DIR)/binwave
LINTER    := $(BIN_DIR)/golangci-lint-$(GOLANGCI_LINT_VERSION)-$(shell go env GOVERSION)
IMAGE     ?= binwave:dev
FUZZTIME  ?= 30s

VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE      ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
# The symbol table and the DWARF information are kept (no -s -w): perf, gdb and every profiler then
# show the names of the functions of a running service, which is worth a few megabytes of binary.
LDFLAGS   := \
	-X github.com/thealexjo/binwave/internal/buildinfo.Version=$(VERSION) \
	-X github.com/thealexjo/binwave/internal/buildinfo.Commit=$(COMMIT) \
	-X github.com/thealexjo/binwave/internal/buildinfo.Date=$(DATE)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the binwave binary into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/binwave

.PHONY: test
test: ## Run unit tests (no Docker required)
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests against ephemeral containers (Docker required)
	go test -race -count=1 -tags integration -timeout 15m ./test/integration/...

.PHONY: test-e2e
test-e2e: ## Run end-to-end tests against ephemeral containers (Docker required)
	go test -race -count=1 -tags e2e -timeout 30m ./test/e2e/...

.PHONY: test-all
test-all: test test-integration test-e2e ## Run every test suite

.PHONY: verify-systemd
verify-systemd: build ## Validate the example systemd unit (systemd-analyze)
	./scripts/verify-systemd.sh $(BINARY)

.PHONY: fuzz
fuzz: ## Run every fuzz test for FUZZTIME each (default 30s)
	@for pkg in $$(go list ./...); do \
		for fuzz in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz'); do \
			echo "==> $$pkg $$fuzz"; \
			go test -run '^$$' -fuzz "^$$fuzz\$$" -fuzztime $(FUZZTIME) $$pkg || exit 1; \
		done; \
	done

.PHONY: docker-build
docker-build: ## Build the Docker image (IMAGE, default binwave:dev)
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t $(IMAGE) .

.PHONY: verify-docker
verify-docker: docker-build ## Smoke-test the Docker image
	./scripts/verify-docker.sh $(IMAGE)

.PHONY: cover
cover: ## Run unit tests with a coverage report
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: $(LINTER) ## Run golangci-lint (all build tags)
	$(LINTER) run --build-tags integration,e2e ./...

.PHONY: fmt
fmt: $(LINTER) ## Format code
	$(LINTER) fmt ./...

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out

# Build the linter with the project toolchain: a linter built with an older Go refuses to analyze newer code.
$(LINTER):
	@mkdir -p $(BIN_DIR)
	GOTOOLCHAIN=$$(go env GOVERSION) GOBIN=$(BIN_DIR) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN_DIR)/golangci-lint $@
