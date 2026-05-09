.PHONY: build test test-integration fmt vet tidy

build:
	go build -o bin/podman-api ./cmd/podman-api

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy
