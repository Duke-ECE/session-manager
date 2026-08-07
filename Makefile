# Standard Makefile (go-repo-rules): binary name = repo dir name,
# `make gates` before every push. GOTOOLCHAIN=local guards against dev
# machines silently pulling a newer toolchain than the golang:1.25
# Docker image accepts.
export GOTOOLCHAIN=local

BINARY := $(notdir $(CURDIR))

build:
	go build -o $(BINARY) ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./...

vet:
	go vet ./...

fmt:
	@gofmt -l .

fmt-fix:
	gofmt -w .

check:
	./scripts/check.sh

gates: fmt vet test check

clean:
	rm -f $(BINARY)

tidy:
	go mod tidy
	@grep '^go ' go.mod

.PHONY: build run test vet fmt fmt-fix check gates clean tidy
