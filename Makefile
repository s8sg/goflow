INTEGRATION_TEST_PATH?=./integration-test
EXAMPLES_PATH?=./samples

.PHONY: build
all: build coverage

build: | __go-pkg-list
	@go build --ldflags "-s -w" -a -installsuffix cgo ${GO_PKG_LIST}

## coverage: Generate global code coverage report
.PHONY: coverage
coverage: | __go-pkg-list unit integration
	@go tool cover -html=/tmp/pls_cp.out -o /tmp/coverage.html
	@echo "You can find coverage report at /tmp/coverage.html"

## coverage: Generate global code coverage report
.PHONY: unit
test.unit: | __go-pkg-list
	@go test -tags=unit -gcflags=-l -v ${GO_PKG_LIST} -coverprofile /tmp/pls_cp.out


## Run integration test
.PHONY: integration
test.integration:
	go test -tags=integration $(INTEGRATION_TEST_PATH) $(EXAMPLES_PATH) -count=1 -v -coverprofile /tmp/pls_cp.out

## docker.start: Starts docker compose
docker.start:
	@docker-compose up -d

## docker.start: Starts docker compose
docker.stop:
	@docker-compose down

__go-pkg-list:
ifeq ($(origin GO_PKG_LIST), undefined)
	$(eval GO_PKG_LIST ?= $(shell go list ./... | grep -v /doc/ | grep -v /template/ | grep -v /vendor/ | grep -v samples | grep -v integration-test))
endif