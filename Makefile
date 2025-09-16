## Set GOPATH if not already set
ifndef GOPATH
	export GOPATH := $(HOME)/go
endif

.PHONY: build fmt lint vet coverage test-unit test-integration test-e2e test-all all
all: fmt lint vet build test-unit

## build: Build the application
.PHONY: build
build: | __go-pkg-list
	@go build ./...

## fmt: Format code using gofmt
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	@gofmt -s -w .
	@echo "Code formatted successfully"

## lint: Run static analysis with golangci-lint and staticcheck
.PHONY: lint
lint: | __go-pkg-list
	@echo "Running golangci-lint..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run ./...
	@echo "Linting completed successfully"

## vet: Run go vet for common errors
.PHONY: vet
vet: | __go-pkg-list
	@echo "Running go vet..."
	@go vet ${GO_PKG_LIST}
	@echo "Vet completed successfully"

## test-unit: Run unit tests only (fast, no external dependencies)
.PHONY: test-unit
test-unit: | __go-pkg-list
	@echo "Running unit tests..."
	@go test -v ${GO_PKG_LIST} -short
	@echo "Unit tests completed successfully"

## test-integration: Run integration tests only (requires Docker)
.PHONY: test-integration
test-integration: | __go-pkg-list
	@echo "Running integration tests..."
	@go test -v ${GO_PKG_LIST} -tags=integration
	@echo "Integration tests completed successfully"

## test-e2e: Run end-to-end tests only (requires full system setup)
.PHONY: test-e2e
test-e2e: | __go-pkg-list
	@echo "Running e2e tests..."
	@go test -v ${GO_PKG_LIST} -tags=e2e
	@echo "E2E tests completed successfully"

## test-all: Run all test types (unit + integration + e2e)
.PHONY: test-all
test-all: | __go-pkg-list
	@echo "Running all tests..."
	@go test -v ${GO_PKG_LIST} -tags="integration,e2e"
	@echo "All tests completed successfully"

## coverage: Generate global code coverage report (unit tests only)
.PHONY: coverage
coverage: | __go-pkg-list
	@echo "Running unit tests with coverage..."
	@go test -gcflags=-l -v ${GO_PKG_LIST} -short -coverprofile /tmp/pls_cp.out || true
	@go tool cover -html=/tmp/pls_cp.out -o /tmp/coverage.html
	@echo "You can find coverage report at /tmp/coverage.html"
	@COVERAGE=$$(go tool cover -func=/tmp/pls_cp.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "Total coverage: $${COVERAGE}%"; \
	if [ $$(echo "$${COVERAGE} < 80" | bc -l) -eq 1 ]; then \
		echo "Coverage $${COVERAGE}% is below the required 80% threshold"; \
		exit 1; \
	else \
		echo "Coverage $${COVERAGE}% meets the required 80% threshold"; \
	fi

## coverage-integration: Generate code coverage report for integration tests
.PHONY: coverage-integration
coverage-integration: | __go-pkg-list
	@echo "Running integration tests with coverage..."
	@go test -gcflags=-l -v ${GO_PKG_LIST} -tags=integration -coverprofile /tmp/integration_cp.out || true
	@go tool cover -html=/tmp/integration_cp.out -o /tmp/integration-coverage.html
	@echo "You can find integration coverage report at /tmp/integration-coverage.html"
	@COVERAGE=$$(go tool cover -func=/tmp/integration_cp.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "Total integration coverage: $${COVERAGE}%"

## coverage-all: Generate combined code coverage report for all tests
.PHONY: coverage-all
coverage-all: | __go-pkg-list
	@echo "Running all tests with coverage..."
	@go test -gcflags=-l -v ${GO_PKG_LIST} -tags="integration,e2e" -coverprofile /tmp/all_cp.out || true
	@go tool cover -html=/tmp/all_cp.out -o /tmp/all-coverage.html
	@echo "You can find combined coverage report at /tmp/all-coverage.html"
	@COVERAGE=$$(go tool cover -func=/tmp/all_cp.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "Total combined coverage: $${COVERAGE}%"; \
	if [ $$(echo "$${COVERAGE} < 80" | bc -l) -eq 1 ]; then \
		echo "Coverage $${COVERAGE}% is below the required 80% threshold"; \
		exit 1; \
	else \
		echo "Coverage $${COVERAGE}% meets the required 80% threshold"; \
	fi



## coverage-view: View coverage report in terminal (w3m/lynx/less)
.PHONY: coverage-view
coverage-view:
	@echo "Opening coverage report in terminal..."
	@if command -v w3m >/dev/null 2>&1; then \
		w3m /tmp/coverage.html; \
	elif command -v lynx >/dev/null 2>&1; then \
		lynx /tmp/coverage.html; \
	else \
		echo "No terminal HTML browser (w3m/lynx) found. Showing raw HTML with less."; \
		less /tmp/coverage.html; \
		printf '\nTip: Install w3m or lynx for a better experience: sudo apt install w3m lynx\n'; \
	fi




## __go-pkg-list: Internal target to set GO_PKG_LIST variable
.PHONY: __go-pkg-list
__go-pkg-list:
ifeq ($(origin GO_PKG_LIST), undefined)
	$(eval GO_PKG_LIST := $(shell go list ./... | grep -v /doc/ | grep -v /template/ | grep -v /vendor/ | grep -v /samples/))
endif