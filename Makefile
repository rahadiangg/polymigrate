# polymigrate — build and test targets

BINARY  := poly
PKG     := github.com/rahadiangg/polymigrate
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X $(PKG)/internal/cli.Version=$(VERSION) \
	-X $(PKG)/internal/cli.Commit=$(COMMIT) \
	-X $(PKG)/internal/cli.BuildDate=$(DATE)

.PHONY: all build install test test-race cover vet fmt tidy integration clean

all: vet test build

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/poly

install:
	go install -trimpath -ldflags "$(LDFLAGS)" ./cmd/poly

test:
	go test ./... -count=1

test-race:
	go test ./... -count=1 -race

cover:
	go test ./... -count=1 -coverprofile=coverage.txt -covermode=atomic
	go tool cover -func=coverage.txt | tail -1

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

# Integration tests require two reachable MongoDB replica sets; see the
# POLY_SRC_URI / POLY_DST_URI env vars consumed by the //go:build integration tests.
integration:
	go test ./... -count=1 -tags=integration

clean:
	rm -rf bin coverage.txt coverage.html
