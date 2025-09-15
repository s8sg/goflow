.PHONY: build fmt lint vet coverage all
all: fmt lint vet build coverage

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
	@echo "Running staticcheck..."
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@staticcheck ${GO_PKG_LIST}
	@echo "Linting completed successfully"

## vet: Run go vet for common errors
.PHONY: vet
vet: | __go-pkg-list
	@echo "Running go vet..."
	@go vet ${GO_PKG_LIST}
	@echo "Vet completed successfully"

## coverage: Generate global code coverage report
.PHONY: coverage
coverage: | __go-pkg-list
	@echo "Running tests with coverage..."
	@go test -gcflags=-l -v ${GO_PKG_LIST} -coverprofile /tmp/pls_cp.out || true
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

__go-pkg-list:
ifeq ($(origin GO_PKG_LIST), undefined)
    $(eval GO_PKG_LIST ?= $(shell go list ./... | grep -v /doc/ | grep -v /template/ | grep -v /vendor/ | grep -v /samples/))
endif