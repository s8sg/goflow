.PHONY: build
all: coverage

## coverage: Generate global code coverage report
.PHONY: coverage
coverage: | __go-pkg-list
	@go test -gcflags=-l -v ${GO_PKG_LIST} -coverprofile /tmp/pls_cp.out || true
	@go tool cover -html=/tmp/pls_cp.out -o /tmp/coverage.html
	@echo "You can find coverage report at /tmp/coverage.html"
__go-pkg-list:
ifeq ($(origin GO_PKG_LIST), undefined)
	$(eval GO_PKG_LIST ?= $(shell go list ./... | grep -v /doc/ | grep -v /template/ | grep -v /vendor/))
endif
