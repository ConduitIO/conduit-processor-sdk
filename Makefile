.PHONY: default
default: fmt lint test

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

.PHONY: generate
generate:
	go generate ./...

.PHONY: proto-generate
proto-generate:
	cd proto && buf generate

.PHONY: proto-lint
proto-lint:
	cd proto && buf lint