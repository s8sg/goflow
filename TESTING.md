# Testing Guide

This project uses three types of tests with proper separation using build tags.

## Test Types

### Unit Tests
**Purpose**: Fast, isolated tests using mocks and in-memory data structures
**Build Tag**: None (run by default)
**Dependencies**: No external services required
**Scope**: Individual functions, methods, and components

### Integration Tests
**Purpose**: Tests that verify interaction between components with real external services
**Build Tag**: `integration`
**Dependencies**: Docker is required to run containers (Redis, etc.)
**Scope**: Component integration, database interactions, external service APIs

### End-to-End (E2E) Tests
**Purpose**: Tests complete user workflows across the entire system
**Build Tag**: `e2e`
**Dependencies**: Docker and full system setup
**Scope**: Complete business scenarios, user journeys, system behavior

## Running Tests

### Unit Tests Only (Default)
```bash
# Run all unit tests (fast, no external dependencies)
go test ./...

# Run unit tests for specific package
go test ./runtime

# Run unit tests with coverage
go test ./... -cover
```

### Integration Tests Only
```bash
# Run all integration tests (slower, requires Docker)
go test ./... -tags=integration

# Run integration tests for specific package
go test ./runtime -tags=integration

# Run integration tests with coverage
go test ./... -tags=integration -cover
```

### End-to-End Tests Only
```bash
# Run all E2E tests (slowest, requires full system)
go test ./... -tags=e2e

# Run E2E tests for specific package
go test ./e2e -tags=e2e

# Run E2E tests with coverage
go test ./... -tags=e2e -cover
```

### Combined Test Runs
```bash
# Run unit + integration tests
go test ./... -tags=integration

# Run all test types (unit + integration + e2e)
go test ./... -tags="integration,e2e"

# Skip integration/e2e tests if running in CI without Docker
go test ./... -short  # integration/e2e tests check testing.Short()
```

## Test Organization

### Files with Build Tags

#### Integration Tests
- `/runtime/flow_runtime_integration_test.go` - Runtime package integration tests

These files have the build constraint:
```go
//go:build integration
// +build integration
```

#### End-to-End Tests
- `/e2e/e2e_test.go` - Complete system end-to-end tests

These files have the build constraint:
```go
//go:build e2e
// +build e2e
```

#### Unit Tests
- All other `*_test.go` files - No build tags, run by default
- Use mocks and in-memory implementations
- Fast execution, no external dependencies

## CI/CD Considerations

### Fast Feedback (Unit Tests)
```bash
# In CI for quick feedback on PRs
go test ./... -short -cover
```

### Full Test Suite (Integration Tests)
```bash
# In CI for release builds or nightly runs
go test ./... -tags=integration -cover
```

### Docker-less Environments
```bash
# When Docker is not available
go test ./... -short
```

## Test Infrastructure

### Testcontainers
- Used for integration tests requiring Redis
- Automatically manages container lifecycle
- Provides isolated test environments

### Gomock
- Generates mocks for interfaces
- Used in unit tests for dependency isolation
- Regenerate mocks: `go generate ./...`

### Build Tags
- `//go:build integration` - Modern Go build constraint
- `// +build integration` - Legacy build constraint (for compatibility)
- Both are included for maximum compatibility

## Coverage Goals

- **Unit Tests**: Should achieve high coverage for business logic
- **Integration Tests**: Should verify external service interactions
- **Combined**: Aim for >80% total coverage with both test types

## Examples

### Running Tests in Development
```bash
# Quick feedback during development
make test-unit  # or: go test ./...

# Full validation before commit
make test-all   # or: go test ./... -tags=integration
```

### Running Tests in CI
```bash
# Stage 1: Fast unit tests
go test ./... -short -cover -coverprofile=unit-coverage.out

# Stage 2: Integration tests (parallel job with Docker)
go test ./... -tags=integration -cover -coverprofile=integration-coverage.out
```